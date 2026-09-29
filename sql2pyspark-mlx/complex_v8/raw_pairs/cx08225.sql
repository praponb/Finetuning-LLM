SELECT operator, warehouse_id AS entity_id
FROM stock_moves
WHERE move_type IS NOT NULL
UNION ALL
SELECT sku_code, id
FROM skus
WHERE weight_kg > 6.4
  AND is_active = FALSE
ORDER BY operator, entity_id
