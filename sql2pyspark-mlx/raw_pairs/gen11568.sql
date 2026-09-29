WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', carrier, status) AS carrier_status_key
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-05-08')
      AND cost <= 63
),
base_shipments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        carrier_status_key,
        DATE_SUB(shipped_date, 81) AS shipped_date_minus_81d
    FROM base
    WHERE delivered_date >= TO_DATE('2023-03-29')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        carrier_status_key,
        SUM(cost) OVER w_ordered AS running_sum_cost,
        MIN(cost) OVER w_ordered AS running_min_cost,
        AVG(cost) OVER w_ordered AS running_avg_cost
    FROM base_shipments
    WINDOW w_ordered AS (PARTITION BY order_id, carrier ORDER BY shipped_date_minus_81d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        delivered_date,
        carrier_status_key,
        running_sum_cost,
        running_min_cost,
        LAG(running_min_cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS prev_running_min_cost,
        LEAD(running_min_cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS next_running_min_cost,
        LEAD(running_sum_cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS next_running_sum_cost,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS seq_no,
        NTILE(5) OVER (PARTITION BY order_id ORDER BY running_sum_cost DESC, id) AS running_sum_cost_bucket
    FROM with_windows
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        running_sum_cost,
        running_sum_cost_bucket,
        NTILE(3) OVER (ORDER BY running_min_cost DESC, id) AS running_min_cost_bucket,
        AVG(prev_running_min_cost) OVER (PARTITION BY DATE_TRUNC('month', delivered_date)) AS delivered_date_period_avg_prev_running_min_cost,
        FIRST_VALUE(running_sum_cost_bucket, TRUE) OVER (ORDER BY running_sum_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_sum_cost_bucket
    FROM with_windows_shipments
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN running_sum_cost >= 470 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_sum_cost_tier,
        ROUND(AVG(running_sum_cost_bucket), 2) AS avg_running_sum_cost_bucket,
        APPROX_PERCENTILE(running_sum_cost_bucket, 0.9) AS running_sum_cost_bucket_p90,
        ROUND(AVG(running_sum_cost_bucket), 2) AS avg_running_sum_cost_bucket_2,
        APPROX_PERCENTILE(first_known_running_sum_cost_bucket, ARRAY(0.25, 0.5, 0.75)) AS first_known_running_sum_cost_bucket_quartiles
    FROM scored
    GROUP BY 1
)
SELECT running_sum_cost_tier
FROM totals
ORDER BY running_sum_cost_tier
LIMIT 20;
