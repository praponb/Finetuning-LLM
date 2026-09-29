SELECT t.id, t.txn_time, a.status
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.is_flagged AND a.account_type = 'SAVINGS'
ORDER BY t.id
