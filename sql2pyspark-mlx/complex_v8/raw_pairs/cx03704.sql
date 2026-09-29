WITH matched AS (
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
screened AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
        )
      AND TO_DATE(s.moved_at) < DATE '2025-02-24'
)
SELECT
    id,
    sku_id,
    moved_at,
    prior_rows_12h,
    prior_unit_price_12h
FROM screened
ORDER BY id DESC, prior_unit_price_12h NULLS LAST
