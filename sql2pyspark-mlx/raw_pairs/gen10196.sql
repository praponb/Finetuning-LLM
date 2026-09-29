WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        quantity * unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        line_total,
        DENSE_RANK() OVER (ORDER BY line_total DESC) AS line_total_dense_rank,
        NTILE(5) OVER (ORDER BY line_total, id) AS line_total_bucket,
        PERCENT_RANK() OVER (ORDER BY line_total DESC NULLS FIRST) AS line_total_percent_rank
    FROM filtered
),
metrics_order_items AS (
    -- Ranking and gap features
    SELECT
        id,
        line_total_dense_rank,
        line_total_bucket,
        SUM(line_total_dense_rank) OVER (ORDER BY line_total_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_line_total_dense_rank,
        RANK() OVER (ORDER BY line_total_dense_rank NULLS LAST) AS line_total_dense_rank_rank,
        DENSE_RANK() OVER (ORDER BY line_total DESC) AS line_total_dense_rank_2,
        LAG(line_total_percent_rank, 1, 0) OVER (ORDER BY line_total, id) AS prev_line_total_percent_rank,
        PERCENT_RANK() OVER (ORDER BY line_total_dense_rank) AS line_total_dense_rank_percent_rank
    FROM metrics
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        line_total_dense_rank,
        line_total_bucket,
        line_total_dense_rank_rank,
        line_total_dense_rank_2,
        prev_line_total_percent_rank,
        prev_line_total_percent_rank + line_total_dense_rank_2 AS prev_line_total_percent_rank_plus_line_total_dense_rank_2
    FROM metrics_order_items
    WHERE running_line_total_dense_rank BETWEEN 18 AND 38
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN line_total_dense_rank_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS line_total_dense_rank_rank_tier,
        APPROX_PERCENTILE(line_total_bucket, ARRAY(0.25, 0.5, 0.75)) AS line_total_bucket_quartiles,
        APPROX_PERCENTILE(line_total_bucket, ARRAY(0.25, 0.5, 0.75)) AS line_total_bucket_quartiles_2,
        MIN(line_total_dense_rank_rank) AS min_line_total_dense_rank_rank
    FROM base
    GROUP BY CASE WHEN line_total_dense_rank_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    line_total_dense_rank_rank_tier,
    line_total_bucket_quartiles,
    line_total_bucket_quartiles_2
FROM summary
ORDER BY line_total_dense_rank_rank_tier DESC NULLS FIRST
