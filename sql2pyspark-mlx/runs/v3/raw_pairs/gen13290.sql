SELECT
    a.balance,
    a.opened_date,
    a.account_type,
    t.amount,
    t.merchant
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE a.account_type != 'LOAN' OR HOUR(t.txn_time) BETWEEN 6 AND 12
ORDER BY opened_date DESC
