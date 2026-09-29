SELECT move_type
FROM stock_moves
EXCEPT
SELECT sku_code
FROM skus
WHERE category = 'Beauty'
  AND LOWER(sku_code) LIKE '%ty%'
