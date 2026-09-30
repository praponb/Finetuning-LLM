SELECT
    subscriptions.plan,
    subscriptions.customer_id,
    subscriptions.start_date,
    customers.is_active,
    customers.credit_limit
FROM subscriptions
INNER JOIN customers ON customers.id = subscriptions.customer_id;
