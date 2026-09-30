WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        MAX(premium) OVER (ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_max_3_prior,
        ROUND(AVG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), holder_id ORDER BY start_date ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        COUNT(*) OVER w_status_end_date_month AS rows_in_status_end_date_month,
        LAST_VALUE(end_date) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), holder_id ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        ROUND(premium / NULLIF(SUM(premium) OVER w_start_date_month, 0), 4) AS premium_share_of_start_date_month,
        SUM(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date)) AS start_date_month_sum_premium,
        MAX(premium) OVER (ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_max_2,
        DENSE_RANK() OVER (ORDER BY start_date DESC NULLS FIRST) AS start_date_dense_rank,
        LAG(premium) OVER (PARTITION BY product, status ORDER BY premium DESC, id) AS prev_premium
    FROM scoped
    WINDOW w_status_end_date_month AS (PARTITION BY status, DATE_TRUNC('MONTH', end_date)), w_start_date_month AS (PARTITION BY DATE_TRUNC('MONTH', start_date))
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(premium_max_3_prior / 5000) * 5000 AS premium_max_3_prior_bucket,
        CONCAT(status, '#', CAST(start_date_dense_rank AS STRING)) AS status_ref,
        CASE WHEN status IN ('EXPIRED', 'CANCELLED') THEN 'group_a' WHEN status = 'LAPSED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM metrics
)
-- Final projection
SELECT
    id,
    product,
    premium,
    status,
    premium_max_3_prior,
    premium_avg_7,
    rows_in_status_end_date_month,
    premium_share_of_start_date_month,
    start_date_month_sum_premium,
    premium_max_2,
    prev_premium,
    premium_max_3_prior_bucket
FROM tagged
ORDER BY status, premium_share_of_start_date_month
