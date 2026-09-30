SELECT c.id, c.segment, c.country FROM customers c WHERE NOT EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.browser = 'Safari') ORDER BY country, id DESC
