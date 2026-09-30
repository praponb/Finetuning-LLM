SELECT status
FROM orders
WHERE channel NOT IN ('mobile', 'store', 'web')
EXCEPT
SELECT status
FROM subscriptions
WHERE MONTH(start_date) IN (5, 6, 8)
ORDER BY status
