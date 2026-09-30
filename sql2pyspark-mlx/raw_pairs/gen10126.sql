SELECT a.id, a.balance, a.account_type
FROM accounts a
WHERE a.opened_date < '2023-10-01' AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id);
