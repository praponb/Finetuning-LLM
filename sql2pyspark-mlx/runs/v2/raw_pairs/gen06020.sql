SELECT MONTH(c.signup_date) AS signup_month, c.is_active, ROUND(STDDEV(s.monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY MONTH(c.signup_date), c.is_active
ORDER BY signup_month DESC;
