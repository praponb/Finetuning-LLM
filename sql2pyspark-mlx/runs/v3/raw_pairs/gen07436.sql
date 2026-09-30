SELECT id, price, EXPLODE(tags) AS tag_name FROM products WHERE stock_qty BETWEEN 206 AND 269;
