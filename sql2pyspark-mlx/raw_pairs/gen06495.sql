SELECT c.id, COUNT(s.id) session_count, MIN(s.started_at) min_started_at FROM customers c INNER JOIN sessions s ON c.id = s.customer_id WHERE c.credit_limit <= 9900 GROUP BY c.id ORDER BY id
