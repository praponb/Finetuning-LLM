SELECT c.country, SUM(CASE WHEN a.status = 'FROZEN' THEN 1 ELSE 0 END) AS frozen_count FROM accounts a INNER JOIN customers c ON c.id = a.customer_id GROUP BY c.country ORDER BY frozen_count
