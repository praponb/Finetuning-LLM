select
    event_type,
    quarter(event_time) as event_quarter,
    page,
    round(stddev(duration_sec), 2) as stddev_duration_sec,
    count(duration_sec) as duration_sec_count
from web_events
where page in ('/product', '/checkout')
group by event_type, quarter(event_time), page
