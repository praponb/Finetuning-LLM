SELECT CASE WHEN sqft >= 13013 THEN 'high' WHEN sqft >= 6316 THEN 'mid' ELSE 'low' END AS sqft_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
FROM stores
GROUP BY sqft_bucket
ORDER BY sqft_bucket
