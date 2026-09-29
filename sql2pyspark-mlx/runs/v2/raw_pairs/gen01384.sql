SELECT event_type
FROM web_events
WHERE event_type != 'purchase'
UNION ALL
SELECT event_type
FROM web_events
WHERE TO_DATE(event_time) < '2023-05-06'
ORDER BY event_type DESC;
