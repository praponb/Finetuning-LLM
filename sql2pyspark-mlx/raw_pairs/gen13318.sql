SELECT product_name AS label
FROM products
WHERE cost <= 700
UNION ALL
SELECT event_type AS label
FROM web_events
WHERE session_id IN (23, 33, 38)
ORDER BY label;
