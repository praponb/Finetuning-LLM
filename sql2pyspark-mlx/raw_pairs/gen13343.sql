SELECT
    CASE WHEN credit_limit >= 15700 THEN 'high' WHEN credit_limit >= 2900 THEN 'mid' ELSE 'low' END AS credit_limit_bucket,
    ROUND(AVG(age), 2) AS avg_age,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(segment))) AS segment_values,
    COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count
FROM customers
WHERE country IN ('DE', 'US') AND LENGTH(name) >= 11
GROUP BY credit_limit_bucket
HAVING COUNT(*) >= 5
ORDER BY segment_values ASC, credit_limit_bucket ASC
LIMIT 20;
