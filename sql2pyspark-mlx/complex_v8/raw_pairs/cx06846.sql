WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        CASE WHEN move_type IN ('TRANSFER', 'INBOUND') THEN 'group_a' WHEN move_type = 'OUTBOUND' THEN 'group_b' ELSE 'other' END AS move_type_group,
        ROUND(unit_price * 0.25, 2) AS unit_price_adj,
        CASE WHEN move_type IN ('INBOUND', 'ADJUST') THEN 'group_a' WHEN move_type = 'TRANSFER' THEN 'group_b' ELSE 'other' END AS move_type_group_2
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND unit_price > 160.8
),
with_activity AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM scoped s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
            WHERE st.unit_price < 273.5
              AND st.operator IN ('op_chan', 'op_bee', 'op_dew')
        )
),
with_activity_2 AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.city <> 'Bangkok'
              AND w.capacity > 4912
        )
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    unit_price_adj,
    move_type_group_2,
    CASE WHEN unit_price_adj >= 412.9 THEN 'large' WHEN unit_price_adj >= 150.8 THEN 'medium' ELSE 'small' END AS unit_price_adj_tier,
    ROUND(qty / NULLIF(unit_price_adj, 0), 3) AS qty_per_unit_price_adj
FROM with_activity_2
ORDER BY unit_price_adj NULLS LAST
