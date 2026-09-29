SELECT id, price, EXPLODE(tags) AS tag_name FROM products WHERE product_name IS NOT NULL
