SELECT id, price, product_name, EXPLODE(tags) AS tag FROM products WHERE supplier_id IN (29, 31, 38);
