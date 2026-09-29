WITH recent_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE MONTH(start_date) IN (1, 5, 7)
),
agg AS (
    SELECT plan, COUNT(CASE WHEN status = 'PAUSED' THEN 1 END) AS paused_count, COUNT(DISTINCT monthly_fee) AS unique_statuss
    FROM recent_subscriptions
    GROUP BY plan
)
SELECT plan, paused_count, unique_statuss
FROM agg
WHERE unique_statuss < 8
ORDER BY paused_count, plan
LIMIT 50
