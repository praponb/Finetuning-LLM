SELECT id, end_date, LENGTH(plan) AS plan_length
FROM subscriptions
WHERE customer_id IN (13, 21, 31, 32)
