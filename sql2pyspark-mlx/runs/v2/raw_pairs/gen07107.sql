SELECT
    id,
    status,
    start_date,
    ROUND(monthly_fee, 2) AS monthly_fee_rounded
FROM subscriptions
WHERE monthly_fee BETWEEN 90 AND 190;
