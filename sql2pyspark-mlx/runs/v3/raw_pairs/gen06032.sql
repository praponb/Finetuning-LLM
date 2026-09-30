SELECT c.id, c.country, COUNT(a.id) AS account_count FROM customers c LEFT OUTER JOIN accounts a ON c.id = a.customer_id GROUP BY c.id, c.country ORDER BY country, account_count DESC
