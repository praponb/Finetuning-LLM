SELECT c.city, COUNT(s.id) AS session_count FROM customers c JOIN sessions s ON s.customer_id = c.id WHERE c.city = 'Bangkok' GROUP BY c.city HAVING COUNT(s.id) >= 3 ORDER BY c.city
