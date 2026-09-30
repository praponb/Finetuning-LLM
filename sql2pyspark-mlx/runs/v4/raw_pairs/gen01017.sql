SELECT id, customer_id, UPPER(plan) AS plan_upper FROM subscriptions WHERE customer_id IS NULL AND monthly_fee < 130 ORDER BY id DESC
