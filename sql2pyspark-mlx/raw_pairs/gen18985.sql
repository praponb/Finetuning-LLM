SELECT end_date
FROM subscriptions
UNION
SELECT signup_date AS end_date
FROM customers
ORDER BY end_date DESC;
