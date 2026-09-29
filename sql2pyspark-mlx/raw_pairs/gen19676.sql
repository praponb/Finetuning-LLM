SELECT *
FROM (
    SELECT CASE WHEN sqft >= 13512 THEN 'high' WHEN sqft >= 8276 THEN 'mid' ELSE 'low' END AS sqft_bucket, ROUND(STDDEV(sqft), 2) AS stddev_sqft, COUNT(*) AS store_count
    FROM stores
    GROUP BY CASE WHEN sqft >= 13512 THEN 'high' WHEN sqft >= 8276 THEN 'mid' ELSE 'low' END
) AS agg
WHERE store_count > 4
ORDER BY store_count DESC
