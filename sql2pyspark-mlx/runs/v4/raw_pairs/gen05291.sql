SELECT c.id, c.name, COUNT(a.id) AS account_count, MAX(a.opened_date) AS last_opened_date FROM customers c LEFT JOIN accounts a ON a.customer_id = c.id GROUP BY c.id, c.name HAVING account_count >= 2
