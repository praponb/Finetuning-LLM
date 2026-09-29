select
    event_type,
    session_id,
    max(duration_sec) - min(duration_sec) as duration_sec_range,
    concat_ws(', ', sort_array(collect_set(event_type))) as event_type_values
from web_events
where event_time < '2023-10-01'
group by event_type, session_id
