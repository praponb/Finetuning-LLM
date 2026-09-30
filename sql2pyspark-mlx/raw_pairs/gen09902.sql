SELECT page, 'web_event' AS origin
FROM web_events
UNION
SELECT segment, 'customer' AS origin
FROM customers
