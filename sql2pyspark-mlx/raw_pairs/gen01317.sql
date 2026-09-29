SELECT id, plan, end_date, customer_id, LENGTH(plan) AS plan_length FROM subscriptions WHERE plan NOT IN ('standard', 'premium');
