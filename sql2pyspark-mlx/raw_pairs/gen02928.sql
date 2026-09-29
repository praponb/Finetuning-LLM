SELECT CASE WHEN duration_sec >= 432 THEN 'high' WHEN duration_sec >= 180 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, SUM(CASE WHEN page = '/cart' THEN 1 ELSE 0 END) AS cart_count, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
FROM web_events
GROUP BY CASE WHEN duration_sec >= 432 THEN 'high' WHEN duration_sec >= 180 THEN 'mid' ELSE 'low' END
HAVING SUM(CASE WHEN page = '/cart' THEN 1 ELSE 0 END) > 79
ORDER BY duration_sec_range;
