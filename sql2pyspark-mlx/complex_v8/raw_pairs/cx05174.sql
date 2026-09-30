SELECT id
FROM warehouses
EXCEPT
SELECT id
FROM stock_moves
WHERE unit_price BETWEEN 180.0 AND 398.5
