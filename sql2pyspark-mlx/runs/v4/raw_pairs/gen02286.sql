SELECT QUARTER(end_date) AS end_quarter, plan, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
FROM subscriptions
GROUP BY end_quarter, plan
HAVING MAX(monthly_fee) - MIN(monthly_fee) < 48
