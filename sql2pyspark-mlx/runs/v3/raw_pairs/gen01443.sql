SELECT c.segment, COUNT(s.id) AS session_count
FROM customers c
JOIN sessions s ON c.id = s.customer_id
GROUP BY c.segment
