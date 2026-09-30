WITH valid AS (
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
        CONCAT_WS(': ', move_type, operator) AS move_type_operator_label,
        INITCAP(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND MONTH(moved_at) IN (1, 2)
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.operator IS NOT NULL
        )
      AND s.sku_id <= 13
)
-- Subtotals with CUBE
SELECT
    sku_id,
    COALESCE(move_type, 'ALL') AS move_type,
    COALESCE(operator, 'ALL') AS operator,
    SUM(unit_price) AS total_unit_price,
    COUNT(*) AS row_count
FROM qualified
GROUP BY CUBE(sku_id, move_type, operator)
ORDER BY sku_id NULLS LAST, move_type NULLS LAST, operator NULLS LAST
