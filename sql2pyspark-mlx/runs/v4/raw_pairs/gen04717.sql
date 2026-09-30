select session_id, page from web_events except select session_id, page from web_events where duration_sec > 123;
