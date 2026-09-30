WITH valid AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref,
        ABS(qty - 139) AS qty_distance
    FROM stock_moves
    WHERE moved_at < TIMESTAMP '2025-02-18 14:00:00'
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.move_type = 'ADJUST'
              AND st.qty < 387
        )
),
qualified AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM without_matches s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.move_type NOT IN ('ADJUST', 'INBOUND')
        )
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(move_type, 'ALL') AS move_type,
    warehouse_id,
    sku_id,
    GROUPING_ID() AS gid,
    SUM(CASE WHEN operator = 'op_ann' THEN qty ELSE 0 END) AS qty_operator_op_ann,
    COUNT(*) AS cnt,
    ROUND(SUM(unit_price), 2) AS total_unit_price
FROM qualified
GROUP BY GROUPING SETS ((move_type, warehouse_id, sku_id), (move_type), ())
ORDER BY move_type NULLS LAST, warehouse_id NULLS LAST, sku_id NULLS LAST
