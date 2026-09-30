SELECT
    t.id,
    t.merchant,
    t.txn_type,
    a.balance,
    a.opened_date,
    c.segment
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE t.merchant = 'Shell'
