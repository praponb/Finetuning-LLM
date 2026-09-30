SELECT id, balance, opened_date FROM accounts WHERE balance <= (SELECT MIN(balance) FROM accounts) ORDER BY balance;
