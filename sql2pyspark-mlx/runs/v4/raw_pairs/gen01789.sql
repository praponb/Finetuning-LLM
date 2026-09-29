SELECT t.id, t.merchant
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE status = 'OPEN')
ORDER BY t.merchant, t.id DESC;
