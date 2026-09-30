WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.unit_price) AS prior_unit_price_12h
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.sku_id, a.moved_at
),
qualified AS (
    -- Keep rows whose sku matches
    SELECT s.*
    FROM joined_ranges s
    WHERE EXISTS (
            SELECT 1
            FROM skus sk
            WHERE sk.id = s.sku_id
              AND NOT ARRAY_CONTAINS(sk.labels, 'bulky')
              AND sk.unit_cost >= 158.7
        )
),
combined_details_sku AS (
    -- Join skus
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        s.prior_rows_12h,
        s.prior_unit_price_12h,
        sk.sku_code,
        sk.weight_kg AS sku_weight_kg,
        sk.labels AS sku_labels
    FROM qualified s
    LEFT JOIN skus sk ON sk.id = s.sku_id
)
SELECT id, measure, metric_value
FROM (
    SELECT id, prior_unit_price_12h, sku_weight_kg
    FROM combined_details_sku
) src
UNPIVOT (
    metric_value FOR measure IN (prior_unit_price_12h, sku_weight_kg)
) unp
ORDER BY id, measure
