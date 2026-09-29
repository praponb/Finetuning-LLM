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
tagged AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(prior_rows_12h, 0) AS prior_rows_12h_filled,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label
    FROM covered
    WHERE sku_id >= 4
),
with_activity AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND sk.sku_code IS NOT NULL
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(prior_rows_12h_filled - AVG(prior_rows_12h_filled) OVER (PARTITION BY YEAR(moved_at), prior_rows_12h), 2) AS prior_rows_12h_filled_vs_moved_at_year_prior_row,
        NTH_VALUE(prior_rows_12h, 2) OVER (ORDER BY prior_qty_12h DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_rows_12h,
        MAX(prior_rows_12h_filled) OVER (PARTITION BY YEAR(moved_at), prior_rows_12h) AS moved_at_year_prior_rows_12h_max_prior_rows_12h_,
        ROW_NUMBER() OVER w_all_by_prior_qty_12h AS seq_no,
        ROUND(CUME_DIST() OVER (ORDER BY prior_qty_12h DESC), 3) AS prior_qty_12h_cume_dist,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY YEAR(moved_at), prior_rows_12h)) AS distinct_sku_id_in_moved_at_year_prior_rows_12h
    FROM with_activity
    WINDOW w_all_by_prior_qty_12h AS (ORDER BY prior_qty_12h DESC, id)
)
SELECT
    id,
    sku_id,
    prior_rows_12h,
    prior_qty_12h,
    moved_at_label,
    prior_rows_12h_filled_vs_moved_at_year_prior_row,
    second_prior_rows_12h,
    moved_at_year_prior_rows_12h_max_prior_rows_12h_,
    seq_no,
    distinct_sku_id_in_moved_at_year_prior_rows_12h,
    CASE WHEN prior_rows_12h_filled >= 6 THEN 'large' WHEN prior_rows_12h_filled >= 3 THEN 'medium' ELSE 'small' END AS prior_rows_12h_filled_tier,
    DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at
FROM windowed
ORDER BY prior_rows_12h, seq_no
