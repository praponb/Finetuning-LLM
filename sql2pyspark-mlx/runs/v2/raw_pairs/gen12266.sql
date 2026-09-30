select page, avg_duration_sec
from (
    select page, avg(duration_sec) avg_duration_sec
    from web_events
    where event_time >= '2024-12-01'
    group by page
) sub
where avg_duration_sec < 460
order by page;
