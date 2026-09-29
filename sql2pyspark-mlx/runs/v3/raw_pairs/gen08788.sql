WITH FilteredStores AS (
    SELECT *
    FROM stores
    WHERE store_name NOT LIKE '%Riversi%'
),
sqft_bucket_summary AS (
    SELECT CASE WHEN sqft >= 13489 THEN 'high' WHEN sqft >= 6730 THEN 'mid' ELSE 'low' END AS sqft_bucket, MAX(sqft) AS max_sqft
    FROM FilteredStores
    GROUP BY CASE WHEN sqft >= 13489 THEN 'high' WHEN sqft >= 6730 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM sqft_bucket_summary
WHERE max_sqft < 12000
ORDER BY sqft_bucket
