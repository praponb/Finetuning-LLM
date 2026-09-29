SELECT move_type AS name, warehouse_id
FROM stock_moves
UNION ALL
SELECT wh_code, id
FROM warehouses
