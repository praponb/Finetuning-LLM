SELECT c.id, COUNT(a.id) AS account_count, COUNT(a.balance) AS balance_count FROM customers c LEFT JOIN accounts a ON c.id = a.customer_id WHERE a.customer_id = 24 GROUP BY c.id;
