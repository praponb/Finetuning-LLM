select
    session_id,
    page,
    avg(duration_sec) as avg_duration_sec,
    round(stddev(duration_sec), 2) as stddev_duration_sec
from web_events
group by session_id, page
having avg(duration_sec) >= 460;
