SELECT t.id, t.is_flagged
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE balance BETWEEN 43000 AND 64000);
