WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price
    FROM stock_moves
    WHERE unit_price < 387.1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY move_type, operator ORDER BY qty, id) AS rn,
        SUM(unit_price) OVER (PARTITION BY operator, warehouse_id) AS operator_warehouse_id_sum_unit_price,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY operator, warehouse_id), 0), 4) AS qty_share_of_operator_warehouse_id
    FROM filtered
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT warehouse_id, move_type, operator_warehouse_id_sum_unit_price
    FROM windowed
) src
PIVOT (
    AVG(operator_warehouse_id_sum_unit_price)
    FOR move_type IN ('OUTBOUND', 'TRANSFER', 'ADJUST', 'INBOUND')
)
ORDER BY warehouse_id
