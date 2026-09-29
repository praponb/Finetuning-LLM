select session_id, round(stddev(duration_sec), 2) as stddev_duration_sec, count(distinct duration_sec) as unique_pages
from web_events
where duration_sec <= 205
group by session_id
having round(stddev(duration_sec), 2) >= 79;
