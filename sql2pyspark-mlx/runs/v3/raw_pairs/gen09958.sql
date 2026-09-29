SELECT s.device, s.browser, c.segment, c.email, c.city FROM sessions s INNER JOIN customers c ON s.customer_id = c.id ORDER BY segment, city;
