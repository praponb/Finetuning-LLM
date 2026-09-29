SELECT CASE WHEN sqft >= 17543 THEN 'high' WHEN sqft >= 6440 THEN 'mid' ELSE 'low' END AS sqft_bucket, SORT_ARRAY(COLLECT_SET(city)) AS city_list, SORT_ARRAY(COLLECT_SET(region)) AS region_list
FROM stores
GROUP BY 1
ORDER BY sqft_bucket DESC
