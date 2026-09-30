SELECT start_date
FROM subscriptions
WHERE start_date IS NOT NULL
UNION ALL
SELECT signup_date AS start_date
FROM customers
WHERE signup_date IS NOT NULL
ORDER BY start_date DESC
