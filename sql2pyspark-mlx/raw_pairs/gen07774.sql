SELECT status, customer_id
FROM subscriptions
EXCEPT
SELECT status, customer_id
FROM subscriptions
WHERE status != 'CANCELLED';
