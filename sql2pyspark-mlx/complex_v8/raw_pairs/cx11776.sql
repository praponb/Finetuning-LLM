SELECT sku_id AS qty, qty, 'move' AS source
FROM stock_moves
WHERE unit_price >= 119.5
UNION ALL
SELECT qty, id AS qty, 'move_2' AS source
FROM stock_moves
ORDER BY source
