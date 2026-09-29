SELECT
    category,
    sku_code,
    unit_cost AS amount,
    'sku' AS source
FROM skus
UNION ALL
SELECT
    operator,
    move_type,
    unit_price,
    'move' AS source
FROM stock_moves
ORDER BY source, sku_code
