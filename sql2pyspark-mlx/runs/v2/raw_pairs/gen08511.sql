SELECT *, TO_DATE(event_time) AS event_time_date, TO_DATE(event_time) AS event_time_date_2
FROM web_events
WHERE event_type IN ('page_view', 'purchase', 'add_to_cart') AND (TO_DATE(event_time) >= '2024-06-01' OR session_id IN (2, 12, 31, 35))
