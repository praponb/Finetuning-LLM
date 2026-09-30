SELECT id, balance, status
FROM accounts
WHERE balance <= (SELECT MIN(balance) FROM accounts)
