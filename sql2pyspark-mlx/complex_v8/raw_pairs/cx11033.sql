SELECT id, operator AS category
FROM stock_moves
WHERE unit_price <= 341.5
UNION
SELECT qty, operator
FROM stock_moves
WHERE qty < 394
  AND sku_id IS NOT NULL
ORDER BY id
