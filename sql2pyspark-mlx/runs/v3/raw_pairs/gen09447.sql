SELECT id, brand, price, EXPLODE(tags) AS tag_name FROM products WHERE stock_qty BETWEEN 106 AND 271
