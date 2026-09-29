SELECT status
FROM orders
UNION
SELECT status
FROM subscriptions
WHERE MONTH(end_date) IN (2, 5, 12)
ORDER BY status;
