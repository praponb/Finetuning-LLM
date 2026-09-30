SELECT CASE WHEN c.credit_limit >= 14400 THEN 'high' WHEN c.credit_limit >= 2700 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, SUM(CASE WHEN s.device = 'tablet' THEN 1 ELSE 0 END) AS tablet_count, SORT_ARRAY(COLLECT_SET(s.device)) AS device_list
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY CASE WHEN c.credit_limit >= 14400 THEN 'high' WHEN c.credit_limit >= 2700 THEN 'mid' ELSE 'low' END;
