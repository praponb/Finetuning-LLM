SELECT CASE WHEN units >= 40 THEN 'high' WHEN units >= 9 THEN 'mid' ELSE 'low' END AS units_bucket, ROUND(AVG(units), 2) AS avg_units, COUNT(revenue) AS revenue_count
FROM store_sales
WHERE revenue >= 3200
GROUP BY units_bucket
ORDER BY units_bucket, avg_units DESC;
