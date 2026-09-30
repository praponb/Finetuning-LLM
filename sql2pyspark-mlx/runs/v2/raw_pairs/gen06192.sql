SELECT s.browser, MIN(we.duration_sec) AS min_duration_sec, COUNT(DISTINCT we.duration_sec) AS unique_pages
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE s.device = 'mobile'
GROUP BY s.browser;
