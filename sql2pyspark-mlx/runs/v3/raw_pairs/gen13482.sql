SELECT
    a.opened_date,
    a.balance,
    a.customer_id,
    t.txn_type,
    t.is_flagged,
    t.txn_time
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
WHERE t.amount >= 2600 AND MONTH(a.opened_date) IN (1, 5, 11)
ORDER BY txn_time DESC, balance DESC
