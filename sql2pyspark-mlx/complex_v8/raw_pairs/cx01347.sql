SELECT operator
FROM stock_moves
INTERSECT ALL
SELECT city
FROM warehouses
