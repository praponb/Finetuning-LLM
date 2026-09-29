SELECT id, balance, status
FROM accounts
WHERE balance >= (SELECT MAX(balance) FROM accounts);
