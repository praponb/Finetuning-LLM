SELECT status, account_type, MAX(balance) AS max_balance FROM accounts WHERE opened_date > '2025-01-14' GROUP BY status, account_type HAVING max_balance > 30000;
