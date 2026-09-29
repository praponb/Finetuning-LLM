SELECT id, balance FROM accounts WHERE balance = (SELECT MIN(balance) FROM accounts)
