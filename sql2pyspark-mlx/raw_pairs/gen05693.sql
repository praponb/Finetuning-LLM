SELECT last_restock AS event_date
FROM inventory
WHERE last_restock IS NOT NULL
UNION ALL
SELECT start_date AS event_date
FROM subscriptions
WHERE start_date IS NOT NULL
