SELECT YEAR(customers.signup_date) AS signup_year, COUNT(CASE WHEN subscriptions.plan = 'enterprise' THEN 1 END) AS enterprise_count
FROM subscriptions
LEFT JOIN customers ON subscriptions.customer_id = customers.id
WHERE customers.name LIKE 'Kan%'
GROUP BY YEAR(customers.signup_date);
