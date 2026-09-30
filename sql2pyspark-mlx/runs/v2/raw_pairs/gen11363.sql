SELECT city, CASE WHEN credit_limit >= 17500 THEN 'high' WHEN credit_limit >= 4100 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM customers
WHERE NOT (city IS NULL AND country <> 'US')
GROUP BY city, CASE WHEN credit_limit >= 17500 THEN 'high' WHEN credit_limit >= 4100 THEN 'mid' ELSE 'low' END
ORDER BY city, credit_limit_bucket
LIMIT 5
