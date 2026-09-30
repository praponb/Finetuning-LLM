SELECT id
FROM warehouses
EXCEPT ALL
SELECT qty
FROM stock_moves
ORDER BY id DESC
