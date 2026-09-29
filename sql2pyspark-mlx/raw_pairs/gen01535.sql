SELECT
    MONTH(customers.signup_date) AS signup_month,
    customers.country,
    COUNT(subscriptions.monthly_fee) AS monthly_fee_count,
    MAX(subscriptions.monthly_fee) - MIN(subscriptions.monthly_fee) AS monthly_fee_range
FROM subscriptions
LEFT JOIN customers ON subscriptions.customer_id = customers.id
GROUP BY MONTH(customers.signup_date), customers.country
HAVING COUNT(subscriptions.monthly_fee) >= 2;
