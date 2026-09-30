SELECT CASE WHEN sqft >= 13649 THEN 'high' WHEN sqft >= 6025 THEN 'mid' ELSE 'low' END AS sqft_bucket, region, AVG(sqft) AS avg_sqft, SUM(sqft) AS total_sqft FROM stores GROUP BY sqft_bucket, region;
