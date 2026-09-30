SELECT id, product_name, brand, tag_name FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name;
