from typing import Any, Dict, List, Tuple, Iterable, Optional
from logging import Logger
from ymmsl import Reference, Operator, Port

import msgpack
import psutil
from libmuscle.mcp.protocol import RequestType
from libmuscle.mcp.tcp_transport_client import TcpTransportClient
from libmuscle.profiling import ProfileEvent

def encode_operator(op: Operator) -> str:
    """Convert an Operator to a MsgPack-compatible value."""
    return op.name

def encode_port(port: Port) -> List[str]:
    """Convert a Port to a MsgPack-compatible value."""
    return [str(port.name), encode_operator(port.operator)]

def encode_profile_event(event: ProfileEvent) -> Any:
    """Converts a ProfileEvent to a list.

    Args:
        event: A profile event

    Returns:
        A list with its attributes, for MMP serialisation.
    """
    if event.start_time is None or event.stop_time is None:
        raise RuntimeError(
                'Incomplete ProfileEvent sent. This is a bug, please'
                ' report it.')

    encoded_port = encode_port(event.port) if event.port else None
    return [
            event.event_type.value,
            event.start_time.nanoseconds, event.stop_time.nanoseconds,
            encoded_port, event.port_length, event.slot,
            event.message_number, event.message_size, event.message_timestamp,
            event.cpu_percent, event.memory_usage]

class MLPClient:
    """The client for the MUSCLE Logging Protocol.

    This class connects to the Manager and communicates with it.
    """
    def __init__(self, location: str, instance_id: Optional[Reference] = None) -> None:
        """Create an MLPClient

        Args:
            location: A connection string of the form hostname:port
            instance_id: The ID of the instance this client is for
        """
        self._transport_client = TcpTransportClient(location)
        self._instance_id = instance_id

    def close(self) -> None:
        """Close the connection

        This closes the connection. After this no other member functions can be called.
        """
        self._transport_client.close()
    
    def submit_profile_events(self, events: Iterable[ProfileEvent]) -> None:
        """Sends profiling events to the manager.

        Args:
            events: The events to send.
        """
        request = [
                RequestType.SUBMIT_PROFILE_EVENTS.value,
                str(self._instance_id),
                [encode_profile_event(e) for e in events]]
        self._call_manager(request)

    def report_usage(self, pids: List[Tuple[str, int]], node_name: str, logger: Logger) -> None:
        """Report usage of resources of processes with given (instance_id, pid)
        on this node.

        Args:
            pids: List of (instance_id, pid) tuples
        """
        if len(pids) == 0:
            """ Nothing to monitor, return """
            return

        usage: Dict[str, Tuple[float, int]] = {}
        for instance_id, pid in pids:
            try:
                process = psutil.Process(pid)
                cpu = process.cpu_percent()
                mem = process.memory_info().vms
                logger.debug(f'PID {pid}: CPU {cpu}%, Memory {mem}')
                usage[instance_id] = (cpu, mem)
            except psutil.NoSuchProcess:
                logger.debug(f'PID {pid}: Process not found')

        if len(usage) < 1:
            """ Nothing to monitor """
            return

        request = [
                RequestType.REPORT_USAGE.value,
                node_name, usage]
        self._call_manager(request)

    def _call_manager(self, request: Any) -> Any:
        """Call the manager and do en/decoding.

        Args:
            request: The request to encode and send

        Returns:
            The decoded response
        """
        encoded_request = msgpack.packb(request, use_bin_type=True)
        response, _ = self._transport_client.call(encoded_request)
        return msgpack.unpackb(response, raw=False)
