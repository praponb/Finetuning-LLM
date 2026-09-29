SELECT CASE WHEN units >= 35 THEN 'high' WHEN units >= 17 THEN 'mid' ELSE 'low' END units_bucket, SUM(units) total_units, RANK() OVER (ORDER BY SUM(units) DESC) AS rnk
FROM store_sales
GROUP BY CASE WHEN units >= 35 THEN 'high' WHEN units >= 17 THEN 'mid' ELSE 'low' END
ORDER BY rnk, 1
LIMIT 5;
