select duration_sec, event_type from web_events where session_id in (1, 5, 6, 34) union all select duration_sec, event_type from web_events where page = '/search'
