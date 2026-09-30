SELECT warehouse_id, unit_price AS amount
FROM stock_moves
WHERE warehouse_id >= 3
  AND sku_id IS NOT NULL
UNION ALL
SELECT id, weight_kg
FROM skus
WHERE is_active = TRUE
