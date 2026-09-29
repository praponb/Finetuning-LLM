SELECT id, status, start_date, monthly_fee, SUM(monthly_fee) OVER (PARTITION BY status ORDER BY start_date, id) AS running_monthly_fee FROM subscriptions
