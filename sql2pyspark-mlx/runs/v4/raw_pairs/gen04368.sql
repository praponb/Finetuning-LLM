SELECT DISTINCT status, plan
FROM subscriptions
WHERE customer_id IS NULL
