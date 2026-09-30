SELECT status, COUNT(CASE WHEN account_type = 'CREDIT' THEN 1 END) AS credit_count, MAX(balance) AS max_balance FROM accounts GROUP BY status HAVING MAX(balance) > 37000 ORDER BY credit_count DESC;
