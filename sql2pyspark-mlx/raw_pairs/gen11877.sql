SELECT tag, ROUND(AVG(price), 2) AS avg_price FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE MONTH(launch_date) IN (2, 4, 11) GROUP BY tag;
