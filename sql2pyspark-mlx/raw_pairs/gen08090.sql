SELECT c.id, c.segment, COUNT(s.id) AS session_count FROM customers c LEFT JOIN sessions s ON c.id = s.customer_id WHERE c.country <> 'SG' GROUP BY c.id, c.segment ORDER BY id DESC, segment;
