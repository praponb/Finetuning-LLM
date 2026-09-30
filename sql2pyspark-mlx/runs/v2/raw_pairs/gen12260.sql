SELECT label, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE brand IN ('Umbrella', 'Stark') GROUP BY label ORDER BY product_count DESC, label
