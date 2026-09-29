SELECT t.id, t.is_flagged
FROM transactions t
WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE status IN ('OPEN', 'CLOSED'))
ORDER BY id DESC;
