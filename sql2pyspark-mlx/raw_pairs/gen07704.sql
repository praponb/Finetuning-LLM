SELECT MONTH(c.signup_date) AS signup_month, SUM(s.monthly_fee) AS total_monthly_fee, COUNT(s.monthly_fee) AS monthly_fee_count
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY MONTH(c.signup_date)
ORDER BY signup_month DESC, total_monthly_fee DESC
LIMIT 50;
