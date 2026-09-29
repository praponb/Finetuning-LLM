SELECT status, customer_id, SUM(CASE WHEN status = 'OPEN' THEN balance ELSE 0 END) AS open_balance, COUNT(DISTINCT balance) AS unique_statuss FROM accounts GROUP BY status, customer_id;
