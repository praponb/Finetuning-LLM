SELECT we.id, we.event_type, s.browser, s.device, HOUR(we.event_time) AS event_time_hour FROM web_events we LEFT JOIN sessions s ON we.session_id = s.id WHERE we.event_type != 'page_view';
