SELECT c.segment, COUNT(s.id) AS session_count FROM customers c LEFT JOIN sessions s ON s.customer_id = c.id GROUP BY c.segment;
