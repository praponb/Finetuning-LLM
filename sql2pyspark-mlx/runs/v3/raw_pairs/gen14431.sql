SELECT label, ROUND(AVG(price), 2) AS avg_price FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE price >= 450 GROUP BY label ORDER BY avg_price DESC, label
