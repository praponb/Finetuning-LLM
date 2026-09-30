SELECT operator
FROM stock_moves
WHERE sku_id IS NOT NULL
INTERSECT ALL
SELECT sku_code
FROM skus
WHERE is_active = TRUE
ORDER BY operator
