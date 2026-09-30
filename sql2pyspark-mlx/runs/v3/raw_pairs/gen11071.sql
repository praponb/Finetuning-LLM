SELECT page
FROM web_events
WHERE event_type = 'add_to_cart'
INTERSECT
SELECT page
FROM web_events
WHERE TO_DATE(event_time) < '2023-12-01'
ORDER BY page
