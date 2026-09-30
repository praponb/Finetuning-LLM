select event_type, session_id, event_time, floor(duration_sec) as duration_sec_whole from web_events where event_time >= '2023-07-01';
