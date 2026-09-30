SELECT start_date, end_date
FROM subscriptions
WHERE customer_id = 29
UNION ALL
SELECT start_date, end_date
FROM subscriptions
WHERE YEAR(start_date) = 2025
ORDER BY start_date;
