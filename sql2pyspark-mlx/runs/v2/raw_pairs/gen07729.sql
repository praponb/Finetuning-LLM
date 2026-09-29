SELECT id, customer_id, UPPER(plan) AS plan_upper
FROM subscriptions
WHERE customer_id IS NOT NULL;
