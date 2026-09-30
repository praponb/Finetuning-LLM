WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.qty) AS prior_qty_1d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        sku_id,
        moved_at,
        prior_qty_1d,
        MAX(prior_qty_1d) OVER (PARTITION BY prior_rows_1d ORDER BY prior_qty_1d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prior_qty_1d_max_7,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id,
        DENSE_RANK() OVER (PARTITION BY prior_rows_1d ORDER BY prior_qty_1d) AS prior_qty_1d_dense_rank,
        LAG(prior_rows_1d) OVER w_prior_rows_1d_by_prior_qty_1_3 AS prev_prior_rows_1d,
        LAG(moved_at) OVER (PARTITION BY prior_rows_1d ORDER BY prior_qty_1d, id) AS prev_moved_at
    FROM in_window
    WINDOW w_prior_rows_1d_by_prior_qty_1_3 AS (PARTITION BY prior_rows_1d ORDER BY prior_qty_1d, id)
)
-- Final projection
SELECT
    id,
    moved_at,
    prior_qty_1d,
    rows_in_sku_id,
    prev_prior_rows_1d,
    prev_moved_at,
    ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(prev_moved_at)) / 60, 1) AS minutes_prev_moved_at_to_moved_at,
    YEAR(prev_moved_at) AS prev_moved_at_year
FROM sequenced
ORDER BY id
