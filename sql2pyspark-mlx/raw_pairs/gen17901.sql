SELECT *
FROM (
    SELECT
        status,
        plan,
        monthly_fee
    FROM subscriptions
) src
PIVOT (
    SUM(monthly_fee) AS amount,
    COUNT(1) AS n
    FOR plan IN ('enterprise', 'basic')
)
