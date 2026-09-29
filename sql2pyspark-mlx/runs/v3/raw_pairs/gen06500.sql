SELECT MONTH(c.signup_date) AS signup_month, COUNT(*) AS num_subscriptions
FROM subscriptions s
RIGHT OUTER JOIN customers c ON s.customer_id = c.id
WHERE c.age IS NULL
GROUP BY MONTH(c.signup_date)
HAVING COUNT(*) > 1
ORDER BY num_subscriptions, signup_month DESC;
