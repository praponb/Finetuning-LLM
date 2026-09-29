SELECT c.id, c.country, COUNT(s.id) AS session_count FROM customers c LEFT OUTER JOIN sessions s ON c.id = s.customer_id GROUP BY c.id, c.country
