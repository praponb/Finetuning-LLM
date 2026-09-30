SELECT id, plan, end_date, monthly_fee, LEAD(monthly_fee, 2, 0) OVER (PARTITION BY plan ORDER BY end_date, id) AS next_monthly_fee FROM subscriptions ORDER BY id, end_date
