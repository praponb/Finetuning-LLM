SELECT t.id, t.amount, t.merchant
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE status IN ('OPEN', 'FROZEN', 'CLOSED'));
