SELECT id, monthly_fee, start_date, end_date, LOWER(plan) AS plan_lower FROM subscriptions WHERE customer_id IN (2, 11, 38)
