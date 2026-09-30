SELECT status, customer_id
FROM subscriptions
WHERE plan = 'basic'
UNION ALL
SELECT status, customer_id
FROM subscriptions
WHERE customer_id IN (15, 21, 34);
