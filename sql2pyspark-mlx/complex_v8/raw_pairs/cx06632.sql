WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        CASE WHEN operator IN ('op_chan', 'op_eve') THEN 'group_a' WHEN operator = 'op_dew' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE qty > 102
)
SELECT sku_id AS entity_id, operator_group AS name
FROM base_moves
EXCEPT
SELECT capacity, city
FROM warehouses
WHERE opened_on < DATE '2025-02-14'
ORDER BY name DESC, entity_id
