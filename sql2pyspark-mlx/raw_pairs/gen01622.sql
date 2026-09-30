SELECT id, opened_date, balance, balance - LAG(balance, 1) OVER (ORDER BY opened_date, id) AS balance_change FROM accounts;
