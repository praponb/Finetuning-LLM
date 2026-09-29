WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.qty) AS prior_qty_12h
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.sku_id, a.moved_at
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY prior_rows_12h, sku_id ORDER BY moved_at DESC, id) AS moved_at_quartile,
        ROUND(CUME_DIST() OVER (PARTITION BY prior_rows_12h, sku_id ORDER BY moved_at DESC), 3) AS moved_at_cume_dist
    FROM covered
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_id,
        s.prior_rows_12h,
        s.prior_qty_12h,
        st.id AS move_id,
        st.qty AS move_qty,
        st.warehouse_id AS move_warehouse_id
    FROM metrics s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        prior_qty_12h - move_qty AS prior_qty_12h_minus_move_qty,
        FLOOR(prior_qty_12h / 500) * 500 AS prior_qty_12h_bucket
    FROM line_level
)
-- Final projection
SELECT
    id,
    sku_id,
    prior_rows_12h,
    prior_qty_12h,
    move_id,
    move_qty,
    move_warehouse_id,
    prior_qty_12h_minus_move_qty,
    prior_qty_12h_bucket,
    ROUND(prior_rows_12h / NULLIF(prior_qty_12h_bucket, 0), 3) AS prior_rows_12h_per_prior_qty_12h_bucket
FROM derived
ORDER BY prior_qty_12h DESC NULLS FIRST
