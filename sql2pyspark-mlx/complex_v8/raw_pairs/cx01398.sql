SELECT id
FROM warehouses
EXCEPT
SELECT id
FROM stock_moves
WHERE sku_id <= 4
ORDER BY id
