select event_month, duration_sec_count, min_event_time
from (
    select date_trunc('MONTH', event_time) event_month, count(duration_sec) duration_sec_count, min(event_time) min_event_time
    from web_events
    group by date_trunc('MONTH', event_time)
) sub
where duration_sec_count >= 5
order by event_month desc
limit 20
