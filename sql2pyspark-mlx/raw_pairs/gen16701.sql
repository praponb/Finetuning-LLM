SELECT a.id, a.status, a.opened_date
FROM accounts a
WHERE a.status IN ('CLOSED', 'FROZEN') AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type = 'FEE')
ORDER BY a.status, id
LIMIT 5
