SELECT s.country, s.browser, c.email, IF(c.is_active, 'Yes', 'No') AS is_active_label FROM sessions s INNER JOIN customers c ON s.customer_id = c.id
