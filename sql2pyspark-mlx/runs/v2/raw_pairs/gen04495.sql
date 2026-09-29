SELECT id, category, price, tag_name FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE product_name LIKE '%Watch%';
