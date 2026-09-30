WITH in_window AS (
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
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, sk.category AS sku_category, sk.labels AS sku_labels
    FROM in_window s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_parent AS (
    -- Join skus
    SELECT
        s.*,
        sk.weight_kg AS sku_weight_kg,
        sk.labels AS sku_labels_2,
        sk.sku_code
    FROM with_details s
    JOIN skus sk ON sk.id = s.sku_id
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_parent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id IS NOT NULL
              AND st.unit_price <= 290.4
        )
)
SELECT id, metric_name, amount
FROM (
    SELECT id, prior_unit_price_12h, sku_weight_kg
    FROM without_matches
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric_name IN (prior_unit_price_12h, sku_weight_kg)
) unp
ORDER BY id, metric_name
