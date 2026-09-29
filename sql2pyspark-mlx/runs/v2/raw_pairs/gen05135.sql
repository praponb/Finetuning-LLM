select id, session_id, count(*) over (partition by session_id) as session_id_count
from web_events;
