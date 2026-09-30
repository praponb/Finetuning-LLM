SELECT
    CASE WHEN s.sqft >= 14871 THEN 'high' WHEN s.sqft >= 7891 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    s.city,
    ROUND((100 * SUM(CASE WHEN o.channel = 'mobile' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_mobile,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(o.status))) AS status_values
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
GROUP BY CASE WHEN s.sqft >= 14871 THEN 'high' WHEN s.sqft >= 7891 THEN 'mid' ELSE 'low' END, s.city
HAVING ROUND((100 * SUM(CASE WHEN o.channel = 'mobile' THEN 1 ELSE 0 END)) / COUNT(*), 1) > 59
ORDER BY sqft_bucket
