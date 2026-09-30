SELECT we.page, s.started_at, s.device, s.customer_id, SUBSTRING(s.browser, 1, 4) AS browser_prefix FROM web_events we INNER JOIN sessions s ON s.id = we.session_id
