WITH joined_ranges AS (
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
annotated AS (
    -- Add calculated columns
    SELECT *, DATE_TRUNC('YEAR', moved_at) AS moved_at_year
    FROM joined_ranges
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY sku_id, prior_rows_12h ORDER BY prior_rows_12h, id) AS seq_no,
        prior_rows_12h - LAG(prior_rows_12h) OVER w_sku_id_prior_rows_12h_by_pri AS prior_rows_12h_change,
        LEAD(prior_rows_12h) OVER (PARTITION BY sku_id, prior_rows_12h ORDER BY prior_rows_12h, id) AS next_prior_rows_12h,
        ROUND((UNIX_TIMESTAMP(moved_at_year) - UNIX_TIMESTAMP(LAG(moved_at_year) OVER (PARTITION BY sku_id, prior_rows_12h ORDER BY prior_rows_12h, id))) / 60, 1) AS minutes_since_prev_moved_at_year,
        LAG(prior_qty_12h) OVER (PARTITION BY sku_id, prior_rows_12h ORDER BY prior_rows_12h, id) AS prev_prior_qty_12h
    FROM annotated
    WINDOW w_sku_id_prior_rows_12h_by_pri AS (PARTITION BY sku_id, prior_rows_12h ORDER BY prior_rows_12h, id)
)
-- Final projection
SELECT
    id,
    sku_id,
    moved_at,
    prior_rows_12h,
    prior_qty_12h,
    moved_at_year,
    seq_no,
    prior_rows_12h_change,
    next_prior_rows_12h,
    prev_prior_qty_12h
FROM sequenced
ORDER BY prior_rows_12h, id
