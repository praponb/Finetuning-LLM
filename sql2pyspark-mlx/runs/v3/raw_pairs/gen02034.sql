select id, page, session_id from web_events where session_id in (5, 19, 28) intersect select id, page, session_id from web_events where event_type = 'purchase'
