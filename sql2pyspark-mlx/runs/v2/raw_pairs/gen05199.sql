SELECT id, stock_qty, SUBSTRING(product_name, 1, 4) AS product_name_prefix
FROM products
ORDER BY product_name_prefix DESC;
