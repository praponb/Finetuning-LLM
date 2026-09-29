WITH scoped AS (
    -- Filter policies
    SELECT *
    FROM policies
    WHERE premium BETWEEN 2033.8 AND 3719.7
      AND product IN ('HOME', 'HEALTH')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY holder_id)) AS distinct_status_in_holder_id,
        LEAD(start_date) OVER (ORDER BY end_date DESC, id) AS next_start_date,
        DENSE_RANK() OVER (ORDER BY premium DESC NULLS FIRST) AS premium_dense_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id), 0), 4) AS premium_share_of_holder_id,
        COUNT(*) OVER (PARTITION BY status, DATE_TRUNC('MONTH', end_date)) AS rows_in_status_end_date_month,
        COUNT(*) OVER (PARTITION BY status, product) AS rows_in_status_product,
        LAG(start_date) OVER (ORDER BY premium DESC NULLS FIRST, id) AS prev_start_date,
        SIZE(COLLECT_SET(holder_id) OVER w_status_end_date_month) AS distinct_holder_id_in_status_end_date_month,
        ROUND(AVG(premium) OVER (PARTITION BY status, DATE_TRUNC('MONTH', end_date)), 2) AS status_end_date_month_avg_premium,
        ROUND(AVG(premium) OVER (PARTITION BY status ORDER BY premium DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_avg_4,
        NTILE(4) OVER (PARTITION BY MONTH(end_date) ORDER BY start_date ASC, id) AS start_date_quartile,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY status, product), 0), 4) AS premium_share_of_status_product,
        ROUND(AVG(premium) OVER (PARTITION BY status, product), 2) AS status_product_avg_premium
    FROM scoped
    WINDOW w_status_end_date_month AS (PARTITION BY status, DATE_TRUNC('MONTH', end_date))
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        FLOOR(premium_dense_rank / 2) * 2 AS premium_dense_rank_bucket
    FROM metrics
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(premium_avg_4) IGNORE NULLS OVER (PARTITION BY distinct_status_in_holder_id ORDER BY next_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium_avg_4,
        SUM(premium_dense_rank) OVER (PARTITION BY product, premium_dense_rank_bucket ORDER BY prev_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_dense_rank,
        MAX(premium_avg_4) OVER (PARTITION BY premium_dense_rank, start_date_quartile) AS premium_dense_rank_start_date_quartile_max_premi,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_status_in_holder_id ORDER BY next_start_date DESC, id), 3) AS next_start_date_cume_dist,
        LEAD(start_date_quartile, 1, 0) OVER (PARTITION BY distinct_status_in_holder_id ORDER BY next_start_date DESC, id) AS next_start_date_quartile
    FROM enriched
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        premium_dense_rank_bucket,
        status,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        COUNT(DISTINCT status) AS distinct_status,
        MIN(next_start_date) AS first_next_start_date
    FROM ranked
    GROUP BY 1, 2
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        premium_dense_rank_bucket,
        status,
        n_rows,
        distinct_ids,
        status_list,
        distinct_status,
        first_next_start_date,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', first_next_start_date)) AS rows_in_first_next_start_date_month,
        ROUND(CUME_DIST() OVER (ORDER BY n_rows DESC), 3) AS n_rows_cume_dist,
        ROUND(AVG(n_rows) OVER (PARTITION BY DATE_TRUNC('MONTH', first_next_start_date)), 2) AS first_next_start_date_month_avg_n_rows
    FROM per_group
)
SELECT
    premium_dense_rank_bucket,
    status,
    n_rows,
    distinct_ids,
    status_list,
    first_next_start_date,
    rows_in_first_next_start_date_month,
    n_rows_cume_dist,
    first_next_start_date_month_avg_n_rows,
    n_rows_cume_dist - n_rows AS n_rows_cume_dist_minus_n_rows,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
FROM ranked_2
ORDER BY first_next_start_date DESC, status_slug NULLS LAST
