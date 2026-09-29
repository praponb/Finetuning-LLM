SELECT id, stock_qty, CONCAT(category, ', ', product_name) AS category_product_name
FROM products
WHERE price > 520;
