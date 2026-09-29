SELECT id, product_name, brand, EXPLODE(tags) AS label FROM products WHERE product_name LIKE 'R%';
