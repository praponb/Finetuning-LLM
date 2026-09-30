SELECT id, status, opened_date, balance, balance - LAG(balance, 2) OVER (PARTITION BY status ORDER BY opened_date, id) AS balance_change FROM accounts WHERE MONTH(opened_date) IN (4, 8, 9)
