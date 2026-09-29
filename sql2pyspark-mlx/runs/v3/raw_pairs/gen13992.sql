SELECT s.browser, MAX(we.event_time) AS last_event_time FROM web_events we LEFT OUTER JOIN sessions s ON we.session_id = s.id WHERE s.country = 'DE' GROUP BY s.browser ORDER BY browser DESC LIMIT 1;
