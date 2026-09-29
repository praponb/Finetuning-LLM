SELECT warehouse_id AS entity_id, id AS entity_id_2
FROM stock_moves
WHERE operator IN ('op_chan', 'op_eve', 'op_dew')
  AND warehouse_id <= 3
UNION
SELECT capacity, id
FROM warehouses
ORDER BY entity_id DESC
