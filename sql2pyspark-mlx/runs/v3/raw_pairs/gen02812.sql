SELECT we.page, s.country FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id WHERE we.duration_sec IS NULL AND HOUR(we.event_time) BETWEEN 11 AND 19
