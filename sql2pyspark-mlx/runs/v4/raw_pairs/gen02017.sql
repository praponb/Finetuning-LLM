SELECT id, brand, price, tag_name FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE stock_qty BETWEEN 150 AND 289
