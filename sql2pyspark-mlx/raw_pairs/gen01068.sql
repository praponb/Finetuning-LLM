SELECT category, SUBSTRING(category, 1, 4) AS category_prefix
FROM products
WHERE stock_qty BETWEEN 265 AND 311 AND stock_qty IS NOT NULL;
