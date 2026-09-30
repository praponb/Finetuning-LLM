SELECT *, CASE WHEN event_type IN ('purchase', 'add_to_cart') THEN 'Group A' ELSE 'Other' END AS event_type_group
FROM web_events
WHERE duration_sec < 303 AND (session_id IN (10, 13, 17, 26) OR duration_sec BETWEEN 182 AND 402)
ORDER BY event_time, 6, id
LIMIT 10
