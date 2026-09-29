WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY start_date NULLS FIRST, id) AS rn
    FROM subscriptions
    QUALIFY rn = 1
)
-- Grouped metrics
SELECT
    CASE WHEN monthly_fee >= 227 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_tier,
    SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee ELSE 0 END) AS expired_monthly_fee,
    COUNT_IF(plan = 'basic') AS basic_events,
    MIN(monthly_fee) AS min_monthly_fee,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    APPROX_PERCENTILE(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles
FROM latest
GROUP BY monthly_fee_tier
