SELECT
    id,
    sku_id,
    unit_price,
    'move' AS source
FROM stock_moves
UNION ALL
SELECT
    sku_id,
    qty,
    unit_price,
    'move_2' AS source
FROM stock_moves
WHERE warehouse_id IS NOT NULL
  AND move_type = 'OUTBOUND'
ORDER BY unit_price DESC
