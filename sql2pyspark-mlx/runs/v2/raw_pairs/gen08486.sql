SELECT s.country, t.duration_sec_range, t.pct_cart
FROM sessions s
INNER JOIN (SELECT session_id, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range, ROUND((100 * SUM(CASE WHEN page = '/cart' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cart
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.duration_sec_range > 67
ORDER BY t.pct_cart;
