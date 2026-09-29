SELECT category, product_name
FROM products
WHERE LOWER(TRIM(product_name)) = 'office chair' AND (brand IS NULL OR cost < 330);
