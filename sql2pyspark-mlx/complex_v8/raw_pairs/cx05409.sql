SELECT id
FROM stock_moves
EXCEPT ALL
SELECT id
FROM warehouses
ORDER BY id
