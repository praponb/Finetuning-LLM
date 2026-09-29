SELECT label, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE brand = 'Stark' GROUP BY label;
