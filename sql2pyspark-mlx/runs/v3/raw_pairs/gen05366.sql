SELECT label, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE ARRAY_CONTAINS(tags, 'eco') GROUP BY label;
