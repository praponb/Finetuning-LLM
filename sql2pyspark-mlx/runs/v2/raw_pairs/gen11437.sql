SELECT
    id,
    customer_id,
    status,
    monthly_fee
FROM subscriptions
WHERE YEAR(end_date) = 2023
