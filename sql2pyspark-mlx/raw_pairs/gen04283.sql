SELECT YEAR(s.started_at) AS started_year, AVG(we.duration_sec) AS avg_duration_sec
FROM web_events we
FULL OUTER JOIN sessions s ON we.session_id = s.id
WHERE HOUR(we.event_time) BETWEEN 6 AND 11
GROUP BY YEAR(s.started_at);
