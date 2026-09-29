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
with_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.sku_id,
        s.moved_at,
        sk.weight_kg AS sku_weight_kg,
        sk.labels AS sku_labels,
        sk.sku_code
    FROM in_window s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM with_details_sku s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.warehouse_id >= 3
        )
)
SELECT
    id,
    sku_id,
    moved_at,
    sku_weight_kg,
    sku_labels,
    LENGTH(TRIM(sku_code)) AS sku_code_len,
    UPPER(TRIM(sku_code)) AS sku_code_clean
FROM screened
