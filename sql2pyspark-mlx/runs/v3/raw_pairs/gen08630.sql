SELECT id, plan, status, monthly_fee, SUM(monthly_fee) OVER (PARTITION BY plan, status) plan_sum_monthly_fee FROM subscriptions WHERE end_date < '2024-10-03'
