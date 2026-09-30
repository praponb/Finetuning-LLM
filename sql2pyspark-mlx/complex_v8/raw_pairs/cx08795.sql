WITH matched AS (
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
with_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_12h,
        s.prior_qty_12h,
        sk.weight_kg AS sku_weight_kg
    FROM matched s
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    id,
    sku_id,
    moved_at,
    prior_rows_12h,
    LEAST(sku_weight_kg, prior_qty_12h) AS least_sku_weight_kg_prior_qty_12h,
    TO_DATE(moved_at) AS moved_at_date
FROM with_details
ORDER BY prior_rows_12h DESC, moved_at
