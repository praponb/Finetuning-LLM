WITH recent_moves AS (
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
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        SUBSTRING(TRIM(move_type), 1, 4) AS move_type_prefix
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 6 AND 10
),
without_matches AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM recent_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.operator IN ('op_ann', 'op_dew')
              AND st.unit_price <= 277.6
        )
)
-- Subtotals with CUBE
SELECT
    operator,
    sku_id,
    warehouse_id,
    COUNT(*) FILTER (WHERE move_type NOT IN ('INBOUND', 'TRANSFER')) AS n_move_type_not_in_inbound_tra
FROM without_matches
GROUP BY CUBE(operator, sku_id, warehouse_id)
