WITH valid AS (
    SELECT *, CONCAT_WS('-', status, plan) status_plan_key
    FROM subscriptions
    WHERE status = 'CANCELLED'
      AND customer_id IS NOT NULL
),
cleaned AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        status_plan_key,
        CAST(monthly_fee AS INT) monthly_fee_int
    FROM valid
    WHERE end_date < '2025-08-01'
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC NULLS LAST, id) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY monthly_fee NULLS LAST) AS rn
        FROM ranked
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    SELECT
        *,
        SUM(monthly_fee_int) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_monthly_fee_int,
        LAG(monthly_fee_int, 1) OVER (PARTITION BY plan, status ORDER BY rank_in_group, id) prev_monthly_fee_int,
        NTILE(3) OVER (PARTITION BY status ORDER BY monthly_fee_int DESC, id) monthly_fee_int_bucket
    FROM latest
),
totals AS (
    SELECT
        DATE_TRUNC('WEEK', start_date) AS week_start,
        CASE WHEN monthly_fee_int_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_int_bucket_tier,
        COUNT(*) FILTER (WHERE status = 'CANCELLED') AS cancelled_rows,
        COUNT(*) AS cnt,
        AVG(prev_monthly_fee_int) AS avg_prev_monthly_fee_int,
        SUM(prev_monthly_fee_int) FILTER (WHERE plan = 'standard') AS standard_total_prev_monthly_fee_int
    FROM with_windows
    GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', start_date), CASE WHEN monthly_fee_int_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('WEEK', start_date)))
    HAVING AVG(prev_monthly_fee_int) > 0
)
SELECT
    week_start,
    monthly_fee_int_bucket_tier,
    cancelled_rows,
    cnt,
    avg_prev_monthly_fee_int,
    standard_total_prev_monthly_fee_int
FROM totals;
