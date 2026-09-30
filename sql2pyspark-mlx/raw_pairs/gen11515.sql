SELECT start_date
FROM subscriptions
UNION
SELECT signup_date AS start_date
FROM customers
WHERE age = 56
ORDER BY start_date
