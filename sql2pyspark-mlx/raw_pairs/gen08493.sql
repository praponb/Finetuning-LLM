SELECT s.browser, SUM(CASE WHEN we.page = '/checkout' THEN 1 ELSE 0 END) AS checkout_count, AVG(we.duration_sec) AS avg_duration_sec
FROM web_events we
FULL OUTER JOIN sessions s ON s.id = we.session_id
JOIN customers c ON s.customer_id = c.id
GROUP BY s.browser
ORDER BY checkout_count;
