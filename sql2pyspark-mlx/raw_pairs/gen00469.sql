SELECT we.id, we.duration_sec, s.browser, s.started_at, DATE_FORMAT(we.event_time, 'yyyy-MM') AS event_time_label FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id;
