SELECT
    subscriptions.start_date,
    customers.age,
    customers.name,
    customers.credit_limit
FROM subscriptions
INNER JOIN customers ON subscriptions.customer_id = customers.id
WHERE subscriptions.plan IN ('standard', 'premium') AND subscriptions.monthly_fee > 180;
