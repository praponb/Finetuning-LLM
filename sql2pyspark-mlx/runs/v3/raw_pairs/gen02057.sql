SELECT YEAR(s.started_at) AS started_year, MAX(we.duration_sec) AS max_duration_sec, ROUND(STDDEV(we.duration_sec), 2) AS stddev_duration_sec
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE HOUR(we.event_time) BETWEEN 10 AND 18
GROUP BY YEAR(s.started_at)
ORDER BY stddev_duration_sec
