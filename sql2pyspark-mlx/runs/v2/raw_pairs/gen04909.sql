SELECT
    t.id,
    t.txn_type,
    a.status,
    a.customer_id,
    a.balance
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE a.status = 'FROZEN'
