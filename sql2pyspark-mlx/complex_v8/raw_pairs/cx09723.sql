WITH covered AS (
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
combined_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.prior_rows_7d,
        s.prior_unit_price_7d,
        sk.labels AS sku_labels,
        sk.category AS sku_category,
        sk.is_active AS sku_is_active
    FROM covered s
    JOIN skus sk ON sk.id = s.sku_id AND NOT ARRAY_CONTAINS(sk.labels, 'express')
)
-- Final projection
SELECT
    id,
    sku_id,
    prior_rows_7d,
    prior_unit_price_7d,
    sku_labels,
    sku_category,
    CASE WHEN sku_is_active THEN 1 ELSE 0 END AS sku_is_active_flag,
    CASE WHEN prior_rows_7d >= 5 THEN 'A' WHEN prior_rows_7d >= 3 THEN 'B' ELSE 'C' END AS prior_rows_7d_tier
FROM combined_details_sku
ORDER BY prior_unit_price_7d, sku_id DESC
