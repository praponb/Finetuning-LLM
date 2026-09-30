SELECT QUARTER(c.signup_date) AS signup_quarter, COUNT(*) AS num_subscriptions
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
GROUP BY QUARTER(c.signup_date)
ORDER BY signup_quarter DESC
