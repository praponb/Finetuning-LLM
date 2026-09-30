SELECT page, ROUND((100 * SUM(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_checkout FROM web_events WHERE duration_sec >= 200 GROUP BY page;
