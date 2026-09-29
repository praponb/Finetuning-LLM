SELECT id, page, CONCAT_WS('-', page, event_type) AS page_event_type_key
FROM web_events
WHERE event_type NOT IN ('click', 'add_to_cart') AND (session_id IN (23, 39) OR event_time >= '2024-10-25')
