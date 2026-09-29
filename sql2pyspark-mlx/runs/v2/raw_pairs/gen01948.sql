SELECT tag, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE price <= 1160 GROUP BY tag ORDER BY product_count DESC, tag
