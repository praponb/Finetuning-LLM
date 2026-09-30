SELECT CASE WHEN price >= 1260 THEN 'high' WHEN price >= 340 THEN 'mid' ELSE 'low' END AS price_bucket, SUM(cost) AS total_cost
FROM products
GROUP BY 1;
