SELECT capacity, id AS ref_id
FROM warehouses
WHERE city IN ('Singapore', 'Kuala Lumpur')
UNION
SELECT qty AS capacity, sku_id AS ref_id
FROM stock_moves
WHERE sku_id <= 14
  AND operator NOT IN ('op_chan', 'op_ann')
