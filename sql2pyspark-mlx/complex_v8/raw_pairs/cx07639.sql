SELECT wh_code, id AS ref_id, 'warehouse' AS source
FROM warehouses
UNION
SELECT operator AS wh_code, id AS ref_id, 'move' AS source
FROM stock_moves
