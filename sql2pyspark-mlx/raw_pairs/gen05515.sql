SELECT id, brand, EXPLODE(tags) AS tag_name FROM products WHERE price <= 1000
