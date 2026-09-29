SELECT SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS web_count
FROM orders;
