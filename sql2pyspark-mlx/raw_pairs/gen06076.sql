SELECT end_date, start_date
FROM subscriptions
WHERE start_date >= '2024-05-01'
UNION ALL
SELECT end_date, start_date
FROM subscriptions
WHERE plan = 'enterprise'
ORDER BY start_date
