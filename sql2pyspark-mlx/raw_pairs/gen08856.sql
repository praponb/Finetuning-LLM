WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        CEIL(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(monthly_fee_whole) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_monthly_fee_whole,
        AVG(monthly_fee_whole) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_monthly_fee_whole,
        COUNT(monthly_fee_whole) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_monthly_fee_whole,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY monthly_fee_whole, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_5_prior
    FROM filtered
),
with_windows_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_hist AS seq_no,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('WEEK', start_date), status ORDER BY last4_avg_monthly_fee_whole, id) AS last4_avg_monthly_fee_whole_bucket,
        FIRST_VALUE(last4_max_monthly_fee_whole, TRUE) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last4_max_monthly_fee_whole,
        SUM(last4_avg_monthly_fee_whole) OVER (PARTITION BY status ORDER BY last4_count_monthly_fee_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last4_avg_monthly_fee_whole
    FROM with_windows
    WHERE status IN ('ACTIVE', 'EXPIRED', 'CANCELLED')
    WINDOW w_hist AS (PARTITION BY status ORDER BY monthly_fee, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee_avg_5_prior DESC NULLS FIRST, id) AS rn
    FROM with_windows_subscriptions
    QUALIFY rn = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN monthly_fee_avg_5_prior >= 250 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_avg_5_prior_tier,
    seq_no,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT_IF(status = 'CANCELLED') AS cancelled_events,
    COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS active_count,
    SIZE(COLLECT_SET(status)) AS num_status,
    GROUPING_ID() AS gid
FROM ranked
GROUP BY ROLLUP (CASE WHEN monthly_fee_avg_5_prior >= 250 THEN 'HIGH' ELSE 'LOW' END, seq_no)
HAVING SIZE(COLLECT_SET(status)) >= 1
