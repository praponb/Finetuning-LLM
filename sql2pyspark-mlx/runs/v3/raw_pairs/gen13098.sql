SELECT id, status, end_date
FROM subscriptions
WHERE customer_id IS NULL
UNION ALL
SELECT id, status, end_date
FROM subscriptions
WHERE start_date > '2024-10-30'
ORDER BY end_date
