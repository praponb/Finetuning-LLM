SELECT c.city, COUNT(a.id) AS account_count, SUM(a.balance) AS total_balance FROM customers c LEFT JOIN accounts a ON a.customer_id = c.id GROUP BY c.city ORDER BY total_balance
