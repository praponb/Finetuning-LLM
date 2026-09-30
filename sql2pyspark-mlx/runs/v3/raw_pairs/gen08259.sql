select page, event_year, min_duration_sec
from (
    select page, year(event_time) as event_year, min(duration_sec) as min_duration_sec
    from web_events
    where duration_sec <= 366
    group by page, year(event_time)
) as t
where min_duration_sec < 180
order by 2 desc
