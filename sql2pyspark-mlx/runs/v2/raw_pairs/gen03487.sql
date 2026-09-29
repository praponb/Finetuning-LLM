SELECT
    id,
    plan,
    status,
    UPPER(status) AS status_upper,
    CAST(monthly_fee AS INT) AS monthly_fee_int
FROM subscriptions
WHERE plan IN ('basic', 'standard', 'premium') AND customer_id = 19;
