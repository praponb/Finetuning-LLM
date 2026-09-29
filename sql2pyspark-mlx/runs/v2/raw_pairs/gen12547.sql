SELECT id, status, CONCAT(status, ' ', account_type) AS status_account_type FROM accounts WHERE NOT (opened_date <= '2024-08-01' AND balance < 51000 AND opened_date < '2024-01-01');
