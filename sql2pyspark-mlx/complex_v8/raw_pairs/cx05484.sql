SELECT id, city
FROM warehouses
EXCEPT
SELECT qty, move_type
FROM stock_moves
WHERE unit_price >= 238.5
ORDER BY id DESC
