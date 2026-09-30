WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LAST_DAY(end_date) AS end_date_month_end,
        DATEDIFF(CURRENT_DATE(), start_date) / 30 AS months_since_start_date
    FROM subscriptions
    WHERE start_date > TO_DATE('2023-06-20')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date_month_end, id) AS seq_no,
        CUME_DIST() OVER (PARTITION BY customer_id, status ORDER BY months_since_start_date NULLS FIRST) AS months_since_start_date_cume_dist
    FROM base
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY start_date NULLS FIRST, id) AS latest_rank
        FROM metrics
    ) t
    WHERE latest_rank = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM deduped a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.start_date >= sub.start_date
       AND a.start_date <= DATE_ADD(sub.end_date, 10)
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN months_since_start_date_cume_dist >= 0.83 THEN 'PREMIUM' ELSE 'STANDARD' END AS months_since_start_date_cume_dist_tier,
        CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
        customer_id,
        COUNT(*) AS row_count,
        SORT_ARRAY(COLLECT_SET(subscription_plan)) AS subscription_plan_list,
        GROUPING(CASE WHEN months_since_start_date_cume_dist >= 0.83 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
    FROM with_window
    GROUP BY ROLLUP (CASE WHEN months_since_start_date_cume_dist >= 0.83 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END, customer_id)
)
SELECT
    months_since_start_date_cume_dist_tier,
    seq_no_tier,
    customer_id,
    row_count,
    subscription_plan_list,
    grouping_level
FROM totals
ORDER BY seq_no_tier DESC
