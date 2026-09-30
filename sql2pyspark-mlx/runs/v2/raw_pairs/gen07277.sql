with recent_web_events as (
    select *, date_format(event_time, 'MMM yyyy') as event_time_label
    from web_events
    where event_type = 'add_to_cart' and event_type in ('search', 'add_to_cart', 'purchase')
),
web_events_stats as (
    select event_type, session_id, sum(duration_sec) as total_duration_sec
    from recent_web_events
    group by event_type, session_id
)
select event_type, session_id
from web_events_stats
where total_duration_sec >= 1800
order by event_type desc;
