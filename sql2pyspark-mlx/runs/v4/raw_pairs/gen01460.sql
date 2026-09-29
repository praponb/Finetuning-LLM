WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY start_date DESC, id ASC) AS dedup_rank
        FROM subscriptions
    ) t
    WHERE dedup_rank = 1
)
SELECT
    DATE_TRUNC('YEAR', start_date) AS year_start,
    CASE WHEN monthly_fee >= 224 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_tier,
    PERCENTILE_APPROX(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles,
    SIZE(COLLECT_SET(plan)) AS num_plans
FROM ranked
GROUP BY 1, 2
ORDER BY year_start ASC;
