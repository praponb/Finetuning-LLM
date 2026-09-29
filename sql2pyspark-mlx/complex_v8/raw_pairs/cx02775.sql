WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.unit_price) AS prior_unit_price_7d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
with_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_7d,
        s.prior_unit_price_7d,
        sk.labels AS sku_labels
    FROM in_window s
    JOIN skus sk ON sk.id = s.sku_id
)
-- Final projection
SELECT
    id,
    sku_id,
    prior_rows_7d,
    prior_unit_price_7d,
    sku_labels
FROM with_details
ORDER BY prior_rows_7d DESC, id DESC
