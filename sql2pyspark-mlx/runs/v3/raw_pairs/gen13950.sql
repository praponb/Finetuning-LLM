SELECT id, balance, opened_date FROM accounts WHERE balance >= (SELECT MAX(balance) FROM accounts);
