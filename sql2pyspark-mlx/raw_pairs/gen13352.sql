SELECT c.city, c.segment, COUNT(*) AS num_rows, MIN(s.balance) AS min_balance FROM accounts s LEFT JOIN customers c ON s.customer_id = c.id GROUP BY c.city, c.segment;
