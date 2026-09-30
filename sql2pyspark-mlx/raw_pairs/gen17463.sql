SELECT id, customer_id, monthly_fee, end_date, LENGTH(status) AS status_length, LENGTH(plan) AS plan_length FROM subscriptions WHERE status = 'PAUSED';
