SELECT
    id,
    start_date,
    monthly_fee,
    ROUND(monthly_fee, 0) AS monthly_fee_rounded
FROM subscriptions
WHERE start_date <= '2023-08-31'
ORDER BY start_date DESC, monthly_fee;
