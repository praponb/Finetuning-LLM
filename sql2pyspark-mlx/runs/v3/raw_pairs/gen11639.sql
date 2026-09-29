SELECT start_date
FROM subscriptions
UNION
SELECT delivered_date AS start_date
FROM shipments
