SELECT QUARTER(s.started_at) AS started_quarter, COUNT(DISTINCT we.duration_sec) AS unique_pages, ROUND(AVG(we.duration_sec), 2) AS avg_duration_sec
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE s.device NOT IN ('tablet', 'desktop')
GROUP BY QUARTER(s.started_at);
