SELECT id, status, end_date, SUBSTRING(plan, 1, 2) AS plan_prefix FROM subscriptions WHERE customer_id IS NULL;
