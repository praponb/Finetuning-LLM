SELECT customer_id, id
FROM subscriptions
EXCEPT ALL
SELECT customer_id, id
FROM subscriptions
WHERE customer_id IS NOT NULL;
