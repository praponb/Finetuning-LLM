SELECT id, balance FROM accounts WHERE balance > (SELECT MAX(balance) FROM accounts);
