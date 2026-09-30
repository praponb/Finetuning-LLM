SELECT
    t.id,
    t.txn_time,
    a.balance,
    a.opened_date
FROM transactions t
JOIN accounts a ON a.id = t.account_id
WHERE t.merchant IN ('Shell', 'Lazada', '7-Eleven')
ORDER BY balance, txn_time, id
LIMIT 10
