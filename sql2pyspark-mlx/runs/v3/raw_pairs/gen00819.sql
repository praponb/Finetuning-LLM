SELECT tag, ROUND(AVG(price), 2) AS avg_price FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE launch_date BETWEEN '2023-08-01' AND '2024-11-26' GROUP BY tag ORDER BY avg_price DESC, tag
