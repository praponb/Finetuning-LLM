SELECT session_id, page, duration_sec, CASE WHEN page = '/cart' THEN 1 ELSE 0 END AS is_cart FROM web_events WHERE event_type = 'click' ORDER BY is_cart DESC
