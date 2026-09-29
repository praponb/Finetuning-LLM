SELECT balance, opened_date, status FROM accounts WHERE (YEAR(opened_date) = 2024 AND balance >= 66000) OR status = 'OPEN' ORDER BY 2 DESC, balance;
