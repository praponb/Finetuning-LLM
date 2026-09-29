SELECT e.*, b.country, b.device, b.browser FROM web_events e INNER JOIN sessions b ON e.session_id = b.id
