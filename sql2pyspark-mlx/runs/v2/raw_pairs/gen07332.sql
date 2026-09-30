select id, session_id, event_time, duration_sec, lag(duration_sec, 2) over (partition by session_id order by event_time, id) prev_duration_sec from web_events;
