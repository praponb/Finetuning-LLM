SELECT id, brand, UPPER(TRIM(product_name)) product_name_normalized
FROM products
ORDER BY product_name_normalized DESC, id DESC
