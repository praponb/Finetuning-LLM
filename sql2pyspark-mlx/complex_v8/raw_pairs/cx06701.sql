WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        LEAST(unit_price, qty) AS least_unit_price_qty
    FROM stock_moves
    WHERE unit_price BETWEEN 130.6 AND 293.2
)
SELECT
    id,
    sku_id,
    move_type,
    qty,
    least_unit_price_qty
FROM prepared_moves
WHERE move_type IN ('OUTBOUND', 'INBOUND', 'TRANSFER')
ORDER BY qty, move_type
