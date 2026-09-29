WITH prepared AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE unit_price < 289.7
),
latest AS (
    -- Keep the latest row per operator, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, warehouse_id ORDER BY moved_at DESC, id DESC) AS rn
        FROM prepared
    ) ranked
    WHERE rn = 1
),
line_level_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        st.id AS move_id,
        st.unit_price AS move_unit_price,
        st.move_type AS move_move_type,
        st.qty AS move_qty,
        st.moved_at AS move_moved_at
    FROM latest s
    JOIN stock_moves st ON st.warehouse_id = s.warehouse_id AND st.move_type IN ('TRANSFER', 'ADJUST', 'OUTBOUND')
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM line_level_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.moved_at < TIMESTAMP '2025-03-08 09:00:00'
        )
      AND s.move_unit_price BETWEEN 99.1 AND 298.9
)
SELECT
    COALESCE(operator, 'ALL') AS operator,
    move_id,
    ROUND(SUM(move_unit_price) / NULLIF(SUM(move_qty), 0), 3) AS move_unit_price_to_move_qty_ratio
FROM screened
GROUP BY GROUPING SETS ((operator, move_id), (operator), (move_id), ())
ORDER BY operator NULLS LAST, move_id NULLS LAST
