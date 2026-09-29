select session_id, id from web_events except distinct select session_id, id from web_events where event_time >= '2024-10-29'
