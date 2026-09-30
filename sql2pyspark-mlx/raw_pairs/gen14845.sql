select * from (select page, event_type, duration_sec from web_events where session_id in (11, 12)) pivot (sum(duration_sec) for event_type in ('search', 'add_to_cart', 'page_view'))
