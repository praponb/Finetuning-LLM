SELECT CASE WHEN sqft >= 13737 THEN 'high' WHEN sqft >= 7538 THEN 'mid' ELSE 'low' END AS sqft_bucket, MAX(sqft) - MIN(sqft) AS sqft_range
FROM stores
GROUP BY CASE WHEN sqft >= 13737 THEN 'high' WHEN sqft >= 7538 THEN 'mid' ELSE 'low' END
ORDER BY sqft_bucket;
