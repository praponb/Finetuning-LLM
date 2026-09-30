SELECT CASE WHEN credit_limit >= 15200 THEN 'high' WHEN credit_limit >= 4100 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, AVG(credit_limit) AS avg_credit_limit
FROM customers
WHERE is_active AND (country IN ('US', 'SG') OR segment = 'SMB')
GROUP BY CASE WHEN credit_limit >= 15200 THEN 'high' WHEN credit_limit >= 4100 THEN 'mid' ELSE 'low' END
ORDER BY avg_credit_limit DESC, credit_limit_bucket
LIMIT 50
