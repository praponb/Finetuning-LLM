WITH matched_keys AS (
    SELECT customer_id, id
    FROM subscriptions
    EXCEPT ALL
    SELECT customer_id, id
    FROM subscriptions
    WHERE plan <> 'basic'
),
reconciled AS (
    SELECT s.*
    FROM subscriptions s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
)
SELECT
    CASE WHEN monthly_fee >= 257 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'premium' THEN 1 END), 0), 2) AS monthly_fee_per_premium,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
FROM reconciled
GROUP BY 1
ORDER BY enterprise_count NULLS LAST;
