SELECT c.id, c.email, t.row_count FROM customers c JOIN (SELECT customer_id, COUNT(*) AS row_count FROM accounts GROUP BY customer_id) t ON c.id = t.customer_id ORDER BY email, row_count
