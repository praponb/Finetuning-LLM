SELECT
    id,
    plan,
    status,
    monthly_fee,
    CASE WHEN monthly_fee >= 268 THEN 'High' WHEN monthly_fee >= 66 THEN 'Medium' ELSE 'Low' END AS monthly_fee_band
FROM subscriptions
ORDER BY monthly_fee_band DESC
