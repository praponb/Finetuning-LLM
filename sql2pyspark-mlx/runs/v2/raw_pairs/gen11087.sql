SELECT id, plan, start_date, monthly_fee, LEAD(monthly_fee, 1) OVER (PARTITION BY plan ORDER BY start_date, id) AS next_monthly_fee FROM subscriptions;
