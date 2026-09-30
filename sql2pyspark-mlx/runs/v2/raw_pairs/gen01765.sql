WITH recent_subscriptions AS (
    SELECT *, LOWER(status) AS status_lower
    FROM subscriptions
    WHERE customer_id IS NOT NULL OR start_date <= '2025-07-01'
),
agg AS (
    SELECT plan, status, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
    FROM recent_subscriptions
    GROUP BY plan, status
)
SELECT *
FROM agg;
