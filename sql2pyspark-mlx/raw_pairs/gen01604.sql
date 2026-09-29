SELECT id, price, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE stock_qty < 109
