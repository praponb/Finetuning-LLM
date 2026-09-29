SELECT
    operator,
    unit_price,
    sku_id AS qty,
    'move' AS src
FROM stock_moves
WHERE qty > 168
UNION ALL
SELECT
    category AS operator,
    unit_cost AS unit_price,
    id AS qty,
    'sku' AS src
FROM skus
WHERE category = 'Toys'
UNION ALL
SELECT
    move_type AS operator,
    unit_price,
    id AS qty,
    'move_3' AS src
FROM stock_moves
WHERE unit_price > 111.7
  AND warehouse_id >= 2
ORDER BY unit_price
