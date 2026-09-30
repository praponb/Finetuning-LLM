SELECT id, balance
FROM accounts
WHERE balance = (SELECT MIN(balance) FROM accounts WHERE customer_id = 23)
ORDER BY id DESC;
