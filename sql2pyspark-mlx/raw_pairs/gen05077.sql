SELECT id, balance, status, account_type FROM accounts WHERE balance = (SELECT MIN(balance) FROM accounts);
