SELECT city, id
FROM warehouses
UNION ALL
SELECT category AS city, id
FROM skus
UNION ALL
SELECT move_type AS city, id
FROM stock_moves
WHERE sku_id <= 19
  AND move_type = 'TRANSFER'
