SELECT operator
FROM stock_moves
EXCEPT
SELECT operator
FROM stock_moves
WHERE unit_price BETWEEN 124.1 AND 398.5
  AND sku_id IS NOT NULL
