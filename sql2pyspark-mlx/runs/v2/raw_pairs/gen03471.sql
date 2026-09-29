WITH filtered_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE end_date >= '2024-02-01' AND customer_id IS NOT NULL
),
agg AS (
    SELECT plan, status, SUM(monthly_fee) AS total_monthly_fee
    FROM filtered_subscriptions
    GROUP BY plan, status
)
SELECT plan, status
FROM agg
WHERE total_monthly_fee > 590
ORDER BY status DESC, plan
LIMIT 3
