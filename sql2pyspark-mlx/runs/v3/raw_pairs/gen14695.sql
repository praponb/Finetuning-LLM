SELECT id, session_id, CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END AS is_purchase
FROM web_events
WHERE event_time >= '2024-05-01' AND page = '/cart'
ORDER BY id DESC
LIMIT 5;
