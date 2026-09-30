select event_time, event_type, duration_sec from web_events where session_id in (4, 9, 28) and duration_sec <= 217;
