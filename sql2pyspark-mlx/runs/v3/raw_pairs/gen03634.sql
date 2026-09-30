SELECT CASE WHEN cost >= 690 THEN 'high' WHEN cost >= 290 THEN 'mid' ELSE 'low' END AS cost_bucket, ROUND(AVG(price), 2) AS avg_price
FROM products
GROUP BY CASE WHEN cost >= 690 THEN 'high' WHEN cost >= 290 THEN 'mid' ELSE 'low' END
HAVING avg_price >= 43
ORDER BY cost_bucket
LIMIT 20;
