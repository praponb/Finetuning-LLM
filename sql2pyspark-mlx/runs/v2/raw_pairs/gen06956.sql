select session_id, case when duration_sec >= 434 then 'high' when duration_sec >= 82 then 'mid' else 'low' end duration_sec_bucket, max(duration_sec) - min(duration_sec) duration_sec_range
from web_events
group by session_id, case when duration_sec >= 434 then 'high' when duration_sec >= 82 then 'mid' else 'low' end;
