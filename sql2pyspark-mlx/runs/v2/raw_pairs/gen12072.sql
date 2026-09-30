SELECT s.browser, s.customer_id, we.page
FROM sessions s
INNER JOIN web_events we ON s.id = we.session_id;
