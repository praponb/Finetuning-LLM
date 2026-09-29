SELECT d.country, src.status, COUNT(*) AS num_rows, MAX(src.balance) AS max_balance FROM accounts src LEFT JOIN customers d ON src.customer_id = d.id GROUP BY d.country, src.status;
