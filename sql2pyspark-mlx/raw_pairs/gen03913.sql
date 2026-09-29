SELECT id, status, opened_date, balance, FIRST_VALUE(balance) OVER (PARTITION BY status ORDER BY opened_date, id) AS first_balance FROM accounts;
