SELECT CASE WHEN credit_limit >= 13400 THEN 'high' WHEN credit_limit >= 6300 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM customers
GROUP BY CASE WHEN credit_limit >= 13400 THEN 'high' WHEN credit_limit >= 6300 THEN 'mid' ELSE 'low' END
ORDER BY credit_limit_bucket DESC
