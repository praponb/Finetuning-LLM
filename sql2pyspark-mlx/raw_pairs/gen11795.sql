SELECT
    t.amount,
    t.txn_time,
    a.balance,
    c.segment
FROM transactions t
FULL OUTER JOIN accounts a ON a.id = t.account_id
LEFT JOIN customers c ON a.customer_id = c.id
WHERE c.segment = 'Corporate';
