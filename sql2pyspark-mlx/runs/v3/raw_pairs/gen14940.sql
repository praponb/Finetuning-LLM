SELECT id, product_name, brand, tag_name FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE stock_qty BETWEEN 180 AND 349
