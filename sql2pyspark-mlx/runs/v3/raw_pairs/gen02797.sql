SELECT s.device, s.customer_id, c.is_active FROM sessions s INNER JOIN customers c ON s.customer_id = c.id WHERE c.country = 'DE' ORDER BY customer_id
