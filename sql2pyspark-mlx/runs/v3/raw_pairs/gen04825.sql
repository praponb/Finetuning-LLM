SELECT id, plan, status, start_date, monthly_fee, LEAD(monthly_fee, 1) OVER (PARTITION BY plan, status ORDER BY start_date, id) AS next_monthly_fee FROM subscriptions
