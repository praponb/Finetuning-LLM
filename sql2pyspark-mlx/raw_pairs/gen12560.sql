SELECT CASE WHEN duration_sec >= 423 THEN 'high' WHEN duration_sec >= 184 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, SUM(CASE WHEN page = '/product' THEN 1 ELSE 0 END) AS product_count
FROM web_events
GROUP BY 1
ORDER BY product_count DESC, duration_sec_bucket ASC
LIMIT 10
