SELECT id, session_id, duration_sec
FROM web_events
WHERE event_type NOT IN ('add_to_cart', 'purchase', 'page_view')
EXCEPT
SELECT id, session_id, duration_sec
FROM web_events
WHERE event_time < '2023-06-03'
