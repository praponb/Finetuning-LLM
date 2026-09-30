WITH base AS (
    SELECT *, TRIM(status) clean_status
    FROM subscriptions
    WHERE start_date >= DATE '2024-02-11'
      AND customer_id IS NOT NULL
      AND start_date BETWEEN '2023-11-25' AND '2025-08-22'
)
SELECT
    CASE WHEN monthly_fee >= 219 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    APPROX_PERCENTILE(monthly_fee, 0.9) AS monthly_fee_p90,
    COUNT(*) FILTER (WHERE status = 'PAUSED') AS paused_rows
FROM base
GROUP BY 1
