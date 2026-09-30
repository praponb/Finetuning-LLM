SELECT start_date AS event_date
FROM subscriptions
WHERE end_date < '2025-07-01'
UNION
SELECT launch_date AS event_date
FROM products
WHERE category = 'Home'
