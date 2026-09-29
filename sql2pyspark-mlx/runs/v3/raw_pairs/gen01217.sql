SELECT CASE WHEN sqft >= 14338 THEN 'high' WHEN sqft >= 4617 THEN 'mid' ELSE 'low' END AS sqft_bucket, MIN(sqft) AS min_sqft, SUM(sqft) AS total_sqft
FROM stores
GROUP BY CASE WHEN sqft >= 14338 THEN 'high' WHEN sqft >= 4617 THEN 'mid' ELSE 'low' END
HAVING COUNT(sqft) > 4
ORDER BY min_sqft DESC;
