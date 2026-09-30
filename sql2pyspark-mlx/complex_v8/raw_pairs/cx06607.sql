SELECT operator AS category
FROM stock_moves
EXCEPT ALL
SELECT sku_code
FROM skus
ORDER BY category
