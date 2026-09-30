WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.sku_id,
        a.moved_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.unit_price) AS prior_unit_price_3d
    FROM stock_moves a
    LEFT JOIN stock_moves b
      ON b.sku_id = a.sku_id
     AND b.moved_at < a.moved_at
     AND b.moved_at >= a.moved_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.sku_id, a.moved_at
),
exploded_detail_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.sku_id,
        s.prior_rows_3d,
        s.prior_unit_price_3d,
        st.id AS move_id,
        st.move_type,
        st.operator AS move_operator
    FROM covered s
    JOIN stock_moves st ON st.sku_id = s.sku_id
)
-- Final projection
SELECT
    id,
    sku_id,
    prior_rows_3d,
    prior_unit_price_3d,
    move_id,
    move_type,
    move_operator,
    INITCAP(TRIM(move_type)) AS move_type_clean
FROM exploded_detail_moves
ORDER BY prior_rows_3d DESC NULLS FIRST, id, move_id
LIMIT 5
