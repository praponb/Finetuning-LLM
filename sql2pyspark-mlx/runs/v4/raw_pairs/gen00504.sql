SELECT
    a.balance,
    a.status,
    t.account_id,
    t.txn_time
FROM accounts a
LEFT OUTER JOIN transactions t ON a.id = t.account_id
WHERE a.customer_id IS NULL OR a.account_type = 'CHECKING';
