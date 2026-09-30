SELECT id, plan, monthly_fee, MAX(monthly_fee) OVER (PARTITION BY plan) plan_max_monthly_fee FROM subscriptions WHERE customer_id IS NULL ORDER BY id DESC, plan DESC;
