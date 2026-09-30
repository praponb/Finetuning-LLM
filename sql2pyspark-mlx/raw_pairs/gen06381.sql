WITH base AS (
    SELECT *
    FROM subscriptions
    WHERE start_date < '2024-08-25'
),
agg AS (
    SELECT
        status,
        plan,
        SUM(monthly_fee) AS total_monthly_fee,
        COUNT(*) AS subscription_count
    FROM base
    GROUP BY status, plan
)
SELECT *
FROM agg
WHERE subscription_count > 6
ORDER BY subscription_count DESC;
