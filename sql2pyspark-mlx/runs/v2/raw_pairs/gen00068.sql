SELECT id, price, brand, tag_name FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE brand IN ('Umbrella', 'Initech', 'Globex');
