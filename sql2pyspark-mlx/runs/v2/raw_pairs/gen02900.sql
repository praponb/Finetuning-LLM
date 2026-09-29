SELECT tag, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE launch_date <= '2025-03-30' GROUP BY tag ORDER BY product_count DESC, tag
