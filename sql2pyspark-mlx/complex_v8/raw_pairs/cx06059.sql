WITH scoped_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        qty + unit_price AS qty_plus_unit_price
    FROM stock_moves
    WHERE operator = 'op_chan'
      AND MONTH(moved_at) IN (1, 3)
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        st.id AS move_id,
        st.operator AS move_operator,
        st.unit_price AS move_unit_price,
        st.move_type
    FROM scoped_moves s
    JOIN stock_moves st ON st.sku_id = s.sku_id AND st.operator IN ('op_chan', 'op_dew', 'op_bee')
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        move_operator,
        sku_id,
        PERCENTILE_APPROX(move_unit_price, 0.5) AS median_move_unit_price,
        ROUND(SUM(move_unit_price), 2) AS total_move_unit_price,
        ROUND(AVG(move_unit_price), 2) AS avg_move_unit_price
    FROM line_level_moves
    GROUP BY move_operator, sku_id
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator IN ('op_chan', 'op_ann', 'op_eve')
              AND st.moved_at < TIMESTAMP '2025-02-18 15:00:00'
        )
),
joined_sku AS (
    -- Attach sku attributes
    SELECT
        s.*,
        sk.labels AS sku_labels,
        sk.sku_code,
        sk.weight_kg AS sku_weight_kg
    FROM without_matches s
    JOIN skus sk ON sk.id = s.sku_id AND sk.category IN ('Electronics', 'Food')
),
without_matches_2 AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM joined_sku s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price <= 330.0
              AND st.move_type = 'OUTBOUND'
        )
      AND s.avg_move_unit_price <= 350.3
)
SELECT
    move_operator,
    sku_id,
    median_move_unit_price,
    total_move_unit_price,
    sku_labels,
    sku_code,
    sku_weight_kg
FROM without_matches_2
ORDER BY median_move_unit_price NULLS LAST, sku_code DESC
