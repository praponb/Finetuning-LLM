SELECT label, ROUND(AVG(price), 2) AS avg_price FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE category IN ('Books', 'Sports', 'Electronics') GROUP BY label;
