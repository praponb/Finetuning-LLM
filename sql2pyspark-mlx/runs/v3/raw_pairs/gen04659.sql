SELECT s.browser, MAX(we.duration_sec) - MIN(we.duration_sec) AS duration_sec_range
FROM web_events we
LEFT JOIN sessions s ON s.id = we.session_id
GROUP BY s.browser
ORDER BY duration_sec_range DESC
