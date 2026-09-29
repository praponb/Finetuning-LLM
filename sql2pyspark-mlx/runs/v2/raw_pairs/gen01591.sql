SELECT DATE_FORMAT(customers.signup_date, 'yyyy-MM') AS signup_month, AVG(subscriptions.monthly_fee) AS avg_monthly_fee
FROM subscriptions
INNER JOIN customers ON subscriptions.customer_id = customers.id
WHERE subscriptions.monthly_fee <= 200
GROUP BY DATE_FORMAT(customers.signup_date, 'yyyy-MM')
