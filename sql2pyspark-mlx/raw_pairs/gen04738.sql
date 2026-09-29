SELECT
    subscriptions.start_date,
    subscriptions.plan,
    customers.city,
    customers.is_active
FROM subscriptions
RIGHT OUTER JOIN customers ON subscriptions.customer_id = customers.id
