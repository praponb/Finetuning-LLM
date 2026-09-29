WITH covered AS (
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
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM covered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.qty >= 90
        )
      AND s.prior_qty_7d BETWEEN 375 AND 1875
)
SELECT
    id,
    sku_id,
    moved_at,
    prior_qty_7d
FROM without_matches
ORDER BY id, moved_at DESC
