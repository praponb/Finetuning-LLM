SELECT CASE WHEN sqft >= 16574 THEN 'high' WHEN sqft >= 4352 THEN 'mid' ELSE 'low' END AS sqft_bucket, AVG(sqft) AS avg_sqft, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values
FROM stores
GROUP BY 1;
