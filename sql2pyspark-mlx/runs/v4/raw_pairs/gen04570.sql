SELECT CASE WHEN sqft >= 16635 THEN 'high' WHEN sqft >= 4677 THEN 'mid' ELSE 'low' END AS sqft_bucket, region, SUM(sqft) AS total_sqft
FROM stores
WHERE city = 'Tokyo'
GROUP BY CASE WHEN sqft >= 16635 THEN 'high' WHEN sqft >= 4677 THEN 'mid' ELSE 'low' END, region
ORDER BY total_sqft DESC, 2 DESC
