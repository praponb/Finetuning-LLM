select session_id, max(duration_sec) as max_duration_sec
from web_events
group by session_id
order by 2 desc;
