select
    session_id,
    sort_array(collect_set(event_type)) as event_type_list,
    max(duration_sec) as max_duration_sec,
    min(event_time) as first_event_time
from web_events
where event_time < '2025-07-01' and event_type = 'page_view' and event_type in ('purchase', 'click')
group by session_id;
