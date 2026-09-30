SELECT
    subscriptions.id,
    subscriptions.start_date,
    subscriptions.monthly_fee,
    customers.signup_date
FROM subscriptions
LEFT OUTER JOIN customers ON subscriptions.customer_id = customers.id;
