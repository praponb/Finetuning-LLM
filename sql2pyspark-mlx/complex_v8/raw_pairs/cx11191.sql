SELECT
    sku_id,
    id,
    unit_price AS value,
    'move' AS origin
FROM stock_moves
UNION
SELECT
    sku_id,
    qty AS id,
    unit_price AS value,
    'move_2' AS origin
FROM stock_moves
WHERE move_type IS NOT NULL
ORDER BY id
