SELECT
    a.account_type,
    a.opened_date,
    a.balance,
    t.is_flagged
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
WHERE NOT (HOUR(t.txn_time) BETWEEN 10 AND 13 AND t.txn_time >= '2023-09-01')
ORDER BY balance
