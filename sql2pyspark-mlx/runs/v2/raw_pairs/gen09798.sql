SELECT we.page, s.browser, s.device FROM web_events we LEFT JOIN sessions s ON s.id = we.session_id WHERE s.device != 'tablet' AND s.device IN ('desktop', 'tablet') ORDER BY device DESC, page DESC
