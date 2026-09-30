SELECT id, plan, end_date, monthly_fee, monthly_fee - LAG(monthly_fee, 2) OVER (PARTITION BY plan ORDER BY end_date, id) AS monthly_fee_change FROM subscriptions
