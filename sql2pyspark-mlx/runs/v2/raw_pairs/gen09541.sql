SELECT *
FROM web_events
WHERE event_type IN ('page_view', 'add_to_cart') AND (session_id IN (17, 29) OR event_time < '2024-02-08');
