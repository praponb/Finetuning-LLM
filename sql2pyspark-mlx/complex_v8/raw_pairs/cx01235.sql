WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        CONCAT(move_type, '#', CAST(id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
      AND unit_price >= 119.5
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM base_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.move_type <> 'TRANSFER'
              AND st.moved_at < TIMESTAMP '2025-03-02 06:00:00'
        )
)
SELECT
    sku_id,
    warehouse_id,
    move_type,
    SUM(unit_price) AS total_unit_price,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    MIN(moved_at) AS first_moved_at
FROM qualified
GROUP BY ROLLUP(sku_id, warehouse_id, move_type)
ORDER BY sku_id NULLS LAST, warehouse_id NULLS LAST, move_type NULLS LAST
