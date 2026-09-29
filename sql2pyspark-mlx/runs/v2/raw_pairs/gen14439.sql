SELECT s.browser, s.customer_id, c.is_active FROM sessions s LEFT JOIN customers c ON s.customer_id = c.id ORDER BY customer_id DESC, s.browser DESC;
