select *
from (
    select
        to_date(event_time) as event_day,
        page,
        sum(duration_sec) as total_duration_sec,
        round(stddev(duration_sec), 2) as stddev_duration_sec
    from web_events
    group by to_date(event_time), page
) as agg
where total_duration_sec >= 2400;
