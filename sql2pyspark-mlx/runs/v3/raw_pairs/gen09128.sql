SELECT status, num_accounts FROM (SELECT status, COUNT(*) AS num_accounts FROM accounts WHERE customer_id IS NULL GROUP BY status) AS t WHERE num_accounts >= 4 ORDER BY status;
