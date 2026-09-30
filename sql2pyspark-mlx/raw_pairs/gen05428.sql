SELECT id, product_name, category FROM products WHERE cost <= 390 UNION SELECT id, product_name, category FROM products WHERE brand IS NOT NULL;
