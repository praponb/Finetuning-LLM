SELECT sku_id
FROM stock_moves
EXCEPT ALL
SELECT capacity
FROM warehouses
WHERE city <> 'Singapore'
  AND capacity >= 4912
ORDER BY sku_id
