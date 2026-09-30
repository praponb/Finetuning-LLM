SELECT tag, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE cost BETWEEN 260 AND 520 GROUP BY tag
