SELECT id, account_type, opened_date, balance, LEAD(balance, 1) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS next_balance FROM accounts;
