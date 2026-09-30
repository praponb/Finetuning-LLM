SELECT id, balance, customer_id
FROM accounts
WHERE balance >= (SELECT MAX(balance) FROM accounts);
