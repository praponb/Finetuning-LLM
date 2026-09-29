WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.qty) AS prior_qty_7d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(prior_rows_7d, 0) AS prior_rows_7d_filled,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
    FROM in_window
    WHERE sku_id IS NOT NULL
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM labeled
    WHERE prior_rows_7d_filled > (SELECT MIN(prior_rows_7d_filled) FROM labeled)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(prior_rows_7d_filled, prior_qty_7d) AS least_prior_rows_7d_filled_prior_qty_7d,
        COALESCE(prior_qty_7d, 0) AS prior_qty_7d_filled
    FROM with_activity
    WHERE moved_at < TIMESTAMP '2025-02-18 14:00:00'
)
SELECT
    prior_rows_7d_filled,
    sku_id,
    GROUPING(prior_rows_7d_filled) AS is_total_prior_rows_7d_filled,
    SUM(prior_qty_7d_filled) AS total_prior_qty_7d_filled,
    MAX(moved_at) AS last_moved_at,
    MIN(moved_at) AS first_moved_at
FROM scored
GROUP BY ROLLUP(prior_rows_7d_filled, sku_id)
ORDER BY prior_rows_7d_filled NULLS LAST, sku_id NULLS LAST
