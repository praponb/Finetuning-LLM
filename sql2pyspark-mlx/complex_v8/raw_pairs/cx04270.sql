WITH filtered AS (
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
        ROUND(unit_price * 1.07, 2) AS unit_price_adj
    FROM stock_moves
    WHERE operator = 'op_bee'
)
SELECT operator AS name
FROM filtered
EXCEPT ALL
SELECT operator AS name
FROM stock_moves
ORDER BY name
