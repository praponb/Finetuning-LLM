SELECT id, status, start_date, monthly_fee, LEAD(monthly_fee, 2) OVER (PARTITION BY status ORDER BY start_date, id) AS next_monthly_fee FROM subscriptions
