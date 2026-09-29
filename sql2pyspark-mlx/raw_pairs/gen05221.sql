SELECT start_date, monthly_fee, CONCAT(plan, ', ', status) plan_status FROM subscriptions WHERE customer_id IS NOT NULL;
