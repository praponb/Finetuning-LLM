SELECT *
FROM (
    SELECT city, CASE WHEN sqft >= 13997 THEN 'high' WHEN sqft >= 2873 THEN 'mid' ELSE 'low' END AS sqft_bucket, MAX(sqft) AS max_sqft
    FROM stores
    GROUP BY city, CASE WHEN sqft >= 13997 THEN 'high' WHEN sqft >= 2873 THEN 'mid' ELSE 'low' END
) AS agg
WHERE max_sqft < 15900
ORDER BY sqft_bucket, city
LIMIT 20;
