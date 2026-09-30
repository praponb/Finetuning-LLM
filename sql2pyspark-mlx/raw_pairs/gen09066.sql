select
    to_date(event_time) event_day,
    page,
    case when duration_sec >= 534 then 'high' when duration_sec >= 130 then 'mid' else 'low' end duration_sec_bucket,
    count(case when event_type = 'page_view' then 1 end) page_view_count,
    round(stddev(duration_sec), 2) stddev_duration_sec
from web_events
group by event_day, page, duration_sec_bucket
having round(stddev(duration_sec), 2) >= 50;
