SELECT CASE WHEN sqft >= 12243 THEN 'high' WHEN sqft >= 8115 THEN 'mid' ELSE 'low' END AS sqft_bucket, SUM(sqft) AS total_sqft, RANK() OVER (ORDER BY SUM(sqft) DESC) AS sqft_rank
FROM stores
GROUP BY CASE WHEN sqft >= 12243 THEN 'high' WHEN sqft >= 8115 THEN 'mid' ELSE 'low' END
ORDER BY total_sqft, sqft_bucket
LIMIT 5;
