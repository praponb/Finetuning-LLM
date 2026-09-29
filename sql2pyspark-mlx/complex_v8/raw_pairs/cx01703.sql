SELECT warehouse_id AS qty
FROM stock_moves
WHERE operator <> 'op_bee'
INTERSECT ALL
SELECT id
FROM skus
WHERE ARRAY_CONTAINS(labels, 'hazmat')
  AND weight_kg <= 21.0
ORDER BY qty
