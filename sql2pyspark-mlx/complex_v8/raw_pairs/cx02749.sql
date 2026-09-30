WITH prepared AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        moved_at + INTERVAL 15 MINUTES AS moved_at_plus_15m
    FROM stock_moves
    WHERE MONTH(moved_at) IN (2, 3)
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.unit_price < 363.6
        )
),
move_type_stats AS (
    -- Median unit_price per move_type
    SELECT move_type, PERCENTILE_APPROX(unit_price, 0.5) AS median_unit_price
    FROM qualified
    GROUP BY move_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_unit_price,
        ROUND(s.unit_price - g.median_unit_price, 2) AS unit_price_minus_median
    FROM qualified s
    JOIN move_type_stats g ON g.move_type = s.move_type
    WHERE s.unit_price >= g.median_unit_price
),
with_children AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.moved_at_plus_15m,
        s.median_unit_price,
        s.unit_price_minus_median,
        st.id AS move_id,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price,
        st.move_type AS move_move_type,
        st.sku_id AS move_sku_id
    FROM vs_group s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
),
combined_details_move AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.unit_price,
        s.moved_at_plus_15m,
        s.median_unit_price,
        s.unit_price_minus_median,
        s.move_id,
        s.move_qty,
        s.move_move_type,
        s.move_sku_id,
        st.move_type AS move_move_type_2,
        st.qty AS move_qty_2,
        st.operator AS move_operator,
        sk.weight_kg AS sku_weight_kg
    FROM with_children s
    JOIN stock_moves st ON st.id = s.move_id
    LEFT JOIN skus sk ON sk.id = s.move_sku_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, move_id, measure, amount
FROM (
    SELECT id, move_id, median_unit_price, sku_weight_kg, unit_price_minus_median
    FROM combined_details_move
) src
UNPIVOT (
    amount FOR measure IN (median_unit_price, sku_weight_kg, unit_price_minus_median)
) unp
ORDER BY id, move_id, measure
