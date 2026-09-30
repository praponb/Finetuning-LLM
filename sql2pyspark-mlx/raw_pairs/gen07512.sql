SELECT
    a.balance,
    a.status,
    a.customer_id,
    t.txn_time,
    t.amount,
    t.merchant
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE a.status IS NOT NULL;
