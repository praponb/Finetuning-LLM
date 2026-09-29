select *
from (
    select session_id, round(stddev(duration_sec), 2) as stddev_duration_sec, sum(duration_sec) as total_duration_sec
    from web_events
    where duration_sec between 225 and 382
    group by session_id
) as x
where total_duration_sec > 1300
order by total_duration_sec, stddev_duration_sec desc;
