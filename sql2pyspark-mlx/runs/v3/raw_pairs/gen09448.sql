SELECT a.id, a.account_type FROM accounts a WHERE a.id IN (SELECT account_id FROM transactions WHERE amount BETWEEN 2300 AND 2800) ORDER BY id, account_type;
