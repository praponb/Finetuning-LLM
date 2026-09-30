SELECT c.is_active, COUNT(*) AS total_count FROM accounts a INNER JOIN customers c ON a.customer_id = c.id GROUP BY c.is_active ORDER BY total_count, is_active LIMIT 3
