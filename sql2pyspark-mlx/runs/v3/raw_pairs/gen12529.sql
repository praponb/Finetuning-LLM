SELECT id, price, product_name, EXPLODE(tags) AS tag FROM products WHERE supplier_id = 13
