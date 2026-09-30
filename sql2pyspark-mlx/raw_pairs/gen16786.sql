select event_month, min_duration_sec, duration_sec_range
from (
    select date_trunc('MONTH', event_time) as event_month, min(duration_sec) as min_duration_sec, max(duration_sec) - min(duration_sec) as duration_sec_range
    from web_events
    where event_time >= '2024-11-01'
    group by date_trunc('MONTH', event_time)
) as agg
where min_duration_sec > 200
