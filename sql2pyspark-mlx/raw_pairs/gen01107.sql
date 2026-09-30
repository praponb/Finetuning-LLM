WITH deduped AS (
    SELECT *
    FROM subscriptions
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee NULLS LAST) = 1
)
SELECT
    CASE WHEN monthly_fee >= 197 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'premium' THEN 1 END), 0), 2) AS monthly_fee_per_premium,
    COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS cancelled_count,
    COUNT_IF(status = 'EXPIRED') AS expired_events,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS monthly_fee_per_enterprise,
    AVG(monthly_fee) AS avg_monthly_fee,
    GROUPING_ID() AS grouping_id
FROM deduped
GROUP BY ROLLUP (CASE WHEN monthly_fee >= 197 THEN 'HIGH' ELSE 'LOW' END)
HAVING COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) > 1
ORDER BY expired_events;
