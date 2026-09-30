SELECT
    a.id,
    a.status,
    a.account_type,
    a.opened_date
FROM accounts a
WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type = 'REFUND')
ORDER BY opened_date, id DESC
