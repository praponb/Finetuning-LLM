SELECT id, product_name, price, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE SIZE(tags) > 2;
