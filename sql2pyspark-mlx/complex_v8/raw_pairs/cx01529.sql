SELECT qty, unit_price AS value
FROM stock_moves
WHERE warehouse_id IS NOT NULL
  AND sku_id IS NOT NULL
UNION ALL
SELECT qty, unit_price AS value
FROM stock_moves
ORDER BY value
