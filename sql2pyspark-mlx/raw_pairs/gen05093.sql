SELECT customer_id, status
FROM subscriptions
INTERSECT
SELECT customer_id, status
FROM subscriptions
WHERE MONTH(end_date) IN (1, 8, 9);
