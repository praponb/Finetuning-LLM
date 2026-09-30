SELECT id, brand, EXPLODE(tags) AS tag FROM products WHERE product_name IS NOT NULL;
