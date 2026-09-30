select event_type, duration_sec, session_id, ceil(duration_sec) as duration_sec_whole from web_events where session_id in (5, 6, 13);
