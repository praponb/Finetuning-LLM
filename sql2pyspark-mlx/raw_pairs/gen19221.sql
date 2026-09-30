SELECT c.country, COUNT(a.id) AS account_count FROM customers c LEFT JOIN accounts a ON c.id = a.customer_id GROUP BY c.country HAVING COUNT(a.id) > 1 ORDER BY account_count;
