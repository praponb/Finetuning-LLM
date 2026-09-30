SELECT
    id,
    plan,
    CAST(monthly_fee AS INT) AS monthly_fee_int,
    DATEDIFF(CURRENT_DATE(), start_date) / 30 AS months_since_start_date
FROM subscriptions
WHERE plan NOT IN ('premium', 'enterprise', 'standard')
