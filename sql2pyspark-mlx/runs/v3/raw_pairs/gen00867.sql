SELECT
    id,
    status,
    plan,
    customer_id
FROM subscriptions
WHERE monthly_fee < 230;
