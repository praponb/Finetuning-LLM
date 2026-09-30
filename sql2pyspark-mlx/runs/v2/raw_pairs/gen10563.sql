SELECT id, start_date, monthly_fee, LAG(monthly_fee, 1) OVER (ORDER BY start_date, id) AS prev_monthly_fee FROM subscriptions;
