SELECT
    subscriptions.start_date,
    subscriptions.monthly_fee,
    customers.is_active,
    customers.country,
    customers.signup_date
FROM subscriptions
RIGHT JOIN customers ON subscriptions.customer_id = customers.id
WHERE customers.email IS NOT NULL OR customers.city = 'London';
