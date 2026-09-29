SELECT a.id, a.balance, a.opened_date
FROM accounts a
WHERE a.account_type IN ('CHECKING', 'LOAN', 'CREDIT') AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id IN (28, 32, 38, 40));
