SELECT status
FROM orders
WHERE customer_id IN (34, 35)
INTERSECT
SELECT status
FROM subscriptions
