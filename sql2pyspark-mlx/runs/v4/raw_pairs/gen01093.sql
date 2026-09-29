SELECT we.page, s.device, c.city
FROM web_events we
INNER JOIN sessions s ON s.id = we.session_id
RIGHT JOIN customers c ON s.customer_id = c.id;
