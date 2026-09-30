SELECT event_time, CASE WHEN page = '/checkout' THEN 1 ELSE 0 END is_checkout FROM web_events WHERE duration_sec != 472 AND event_type IN ('purchase', 'page_view') ORDER BY is_checkout ASC;
