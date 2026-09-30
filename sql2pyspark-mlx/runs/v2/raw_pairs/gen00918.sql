SELECT tag, ROUND(AVG(price), 2) AS avg_price FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE supplier_id IS NOT NULL GROUP BY tag ORDER BY avg_price DESC, tag
