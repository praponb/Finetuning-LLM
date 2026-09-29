select event_type from web_events where event_type in ('page_view', 'add_to_cart') union select event_type from web_events where session_id = 8;
