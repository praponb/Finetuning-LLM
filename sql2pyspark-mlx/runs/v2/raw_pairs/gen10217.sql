SELECT
    id,
    plan,
    monthly_fee,
    start_date,
    INITCAP(TRIM(plan)) AS plan_title,
    CASE WHEN monthly_fee >= 258 THEN 'High' WHEN monthly_fee >= 96 THEN 'Medium' ELSE 'Low' END AS monthly_fee_level
FROM subscriptions
WHERE customer_id IN (7, 12, 25)
ORDER BY monthly_fee DESC
