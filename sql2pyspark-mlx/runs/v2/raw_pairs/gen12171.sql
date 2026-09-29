SELECT s.id, s.country, t.pct_checkout
FROM sessions s
JOIN (SELECT session_id, ROUND((100 * SUM(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_checkout
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.pct_checkout >= 31;
