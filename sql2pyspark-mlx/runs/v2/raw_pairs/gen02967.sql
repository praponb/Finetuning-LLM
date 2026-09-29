SELECT c.segment, COUNT(CASE WHEN s.country = 'SG' THEN 1 END) sg_count FROM sessions s LEFT JOIN customers c ON s.customer_id = c.id GROUP BY c.segment;
