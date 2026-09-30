SELECT category, sku_code AS label, 'sku' AS source
FROM skus
WHERE unit_cost > 158.7
  AND weight_kg > 12.4
UNION ALL
SELECT move_type AS category, operator AS label, 'move' AS source
FROM stock_moves
WHERE sku_id <= 17
ORDER BY source DESC
