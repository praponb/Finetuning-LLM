select id, session_id, duration_sec, event_time, substring(page, 1, 3) as page_prefix from web_events where duration_sec <= 380 and event_type in ('purchase', 'add_to_cart', 'page_view')
