SELECT id, price, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE product_name LIKE 'Wa%';
