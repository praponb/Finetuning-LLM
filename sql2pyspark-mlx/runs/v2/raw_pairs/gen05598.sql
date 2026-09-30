SELECT customer_id, status, COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count FROM accounts WHERE customer_id IN (8, 14) AND status = 'OPEN' GROUP BY 1, 2
