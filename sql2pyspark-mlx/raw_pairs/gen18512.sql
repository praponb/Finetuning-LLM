SELECT
    status,
    plan,
    customer_id,
    CASE WHEN monthly_fee >= 208 THEN 'High' WHEN monthly_fee >= 37 THEN 'Medium' ELSE 'Low' END AS monthly_fee_band
FROM subscriptions
WHERE customer_id IS NULL
