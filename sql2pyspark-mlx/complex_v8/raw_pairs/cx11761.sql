WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE operator IN ('op_eve', 'op_dew', 'op_chan')
      AND warehouse_id <= 3
),
with_parent_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city
    FROM cleaned_moves s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    warehouse_id,
    COALESCE(warehouse_city, 'ALL') AS warehouse_city,
    COALESCE(move_type, 'ALL') AS move_type,
    ROUND(100 * COUNT(CASE WHEN operator = 'op_bee' THEN 1 END) / COUNT(*), 1) AS pct_operator_op_bee
FROM with_parent_warehouse
GROUP BY GROUPING SETS ((warehouse_id, warehouse_city, move_type), (warehouse_id), (warehouse_city), ())
