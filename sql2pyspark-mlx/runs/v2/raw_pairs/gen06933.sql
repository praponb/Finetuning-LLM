SELECT id, status, end_date, monthly_fee, LEAD(monthly_fee, 1, 0) OVER (PARTITION BY status ORDER BY end_date, id) AS next_monthly_fee FROM subscriptions;
