SELECT id, balance, status FROM accounts WHERE balance = (SELECT MIN(balance) FROM accounts WHERE YEAR(opened_date) >= 2024)
