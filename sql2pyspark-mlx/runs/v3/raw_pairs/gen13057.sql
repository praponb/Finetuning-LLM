SELECT CASE WHEN sqft >= 12423 THEN 'high' WHEN sqft >= 6115 THEN 'mid' ELSE 'low' END AS sqft_bucket, city, SUM(CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END) AS tokyo_count
FROM stores
WHERE (LOWER(store_name) LIKE '%own%' AND UPPER(store_name) LIKE '%IAM%') OR city IN ('Berlin', 'New York')
GROUP BY sqft_bucket, city;
