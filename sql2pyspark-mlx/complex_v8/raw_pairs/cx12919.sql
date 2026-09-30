WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        CASE WHEN operator IN ('op_dew', 'op_chan') THEN 'group_a' WHEN operator = 'op_bee' THEN 'group_b' ELSE 'other' END AS operator_group,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown
    FROM stock_moves
    WHERE sku_id IS NOT NULL
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        move_type_or_unknown,
        warehouse_id,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        SUM(qty) AS total_qty
    FROM cleaned
    GROUP BY 1, 2
),
with_details AS (
    -- Join warehouses
    SELECT s.*, w.opened_on AS warehouse_opened_on, w.city AS warehouse_city
    FROM summary s
    JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    move_type_or_unknown,
    total_qty,
    warehouse_opened_on,
    warehouse_city,
    DATE_FORMAT(warehouse_opened_on, 'yyyy-MM') AS warehouse_opened_on_label
FROM with_details
WHERE warehouse_opened_on < DATE '2025-07-03'
ORDER BY total_qty DESC
