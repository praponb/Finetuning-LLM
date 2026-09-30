SELECT s.country, MIN(we.duration_sec) AS min_duration_sec
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
GROUP BY s.country
ORDER BY min_duration_sec DESC, country
LIMIT 10;
