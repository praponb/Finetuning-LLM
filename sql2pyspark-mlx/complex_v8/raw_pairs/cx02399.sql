SELECT warehouse_id
FROM stock_moves
INTERSECT ALL
SELECT capacity AS warehouse_id
FROM warehouses
WHERE capacity >= 11005
ORDER BY warehouse_id DESC
