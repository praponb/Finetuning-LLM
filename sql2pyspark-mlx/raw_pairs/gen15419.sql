SELECT id, price, tag_name FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE stock_qty IS NULL
