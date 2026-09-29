WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        sqft,
        AVG(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_sqft,
        MIN(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_sqft,
        MAX(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_sqft
    FROM stores
    WHERE region NOT IN ('East', 'North')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        sqft,
        running_avg_sqft,
        running_max_sqft,
        LAG(running_min_sqft) OVER (ORDER BY running_min_sqft, id) prev_running_min_sqft,
        LEAD(running_min_sqft) OVER (ORDER BY running_min_sqft, id) next_running_min_sqft,
        LEAD(running_max_sqft) OVER (ORDER BY running_min_sqft, id) next_running_max_sqft,
        ROW_NUMBER() OVER (ORDER BY running_min_sqft, id) seq_no
    FROM with_windows
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(next_running_max_sqft) OVER (ORDER BY next_running_min_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_next_running_max_sqft,
        NTILE(4) OVER w_recent next_running_min_sqft_bucket
    FROM scored
    WINDOW w_recent AS (ORDER BY next_running_min_sqft, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.discount IS NOT NULL
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.shipping_fee >= 22
        )
)
-- Ranking and gap features
SELECT
    id,
    sqft,
    running_max_sqft,
    prev_running_min_sqft,
    next_running_min_sqft,
    next_running_max_sqft,
    seq_no,
    cumulative_next_running_max_sqft,
    SUM(sqft) OVER (ORDER BY prev_running_min_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft,
    LAG(next_running_min_sqft, 2) OVER (ORDER BY running_max_sqft, id) prev_next_running_min_sqft,
    DENSE_RANK() OVER (ORDER BY running_avg_sqft DESC NULLS FIRST) running_avg_sqft_dense_rank
FROM eligible
