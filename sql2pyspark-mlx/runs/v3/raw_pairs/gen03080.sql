select
    id,
    event_type,
    session_id,
    page
from web_events
where event_time >= '2025-05-01' and event_time < '2023-10-24'
