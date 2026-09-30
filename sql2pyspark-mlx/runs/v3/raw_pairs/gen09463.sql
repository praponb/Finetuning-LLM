SELECT
    id,
    customer_id,
    plan,
    DATE_TRUNC('week', end_date) AS end_date_period
FROM subscriptions
WHERE plan = 'standard'
