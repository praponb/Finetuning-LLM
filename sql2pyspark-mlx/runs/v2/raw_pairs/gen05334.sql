SELECT s.device, COUNT(DISTINCT we.duration_sec) AS unique_pages FROM web_events we INNER JOIN sessions s ON we.session_id = s.id WHERE we.event_type = 'click' GROUP BY s.device ORDER BY device DESC;
