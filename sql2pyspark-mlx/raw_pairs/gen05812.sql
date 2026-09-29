SELECT
    id,
    start_date,
    status,
    CAST(monthly_fee AS INT) AS monthly_fee_int
FROM subscriptions
WHERE monthly_fee > 170
