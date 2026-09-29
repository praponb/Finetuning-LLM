SELECT id, plan, end_date, monthly_fee, LAG(monthly_fee, 1) OVER (PARTITION BY plan ORDER BY end_date, id) AS prev_monthly_fee FROM subscriptions WHERE monthly_fee BETWEEN 120 AND 220
