SELECT c.id, c.country, COUNT(a.id) AS account_count FROM customers c LEFT JOIN accounts a ON a.customer_id = c.id GROUP BY c.id, c.country ORDER BY id DESC
