SELECT a.status, t.account_id
FROM accounts a
INNER JOIN transactions t ON a.id = t.account_id
WHERE a.account_type = 'SAVINGS';
