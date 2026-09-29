SELECT capacity AS entity_id
FROM warehouses
EXCEPT ALL
SELECT sku_id AS entity_id
FROM stock_moves
WHERE unit_price > 136.0
  AND operator IS NOT NULL
ORDER BY entity_id
