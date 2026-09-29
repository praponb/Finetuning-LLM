SELECT id, sku_id AS entity_id
FROM stock_moves
WHERE unit_price >= 205.1
UNION ALL
SELECT id, capacity AS entity_id
FROM warehouses
WHERE capacity <= 11506
UNION ALL
SELECT sku_id AS id, id AS entity_id
FROM stock_moves
WHERE move_type <> 'TRANSFER'
  AND unit_price <= 277.6
ORDER BY id, entity_id
