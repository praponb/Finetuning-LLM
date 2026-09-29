SELECT
    a.status,
    t.txn_type,
    t.account_id,
    t.merchant
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
WHERE t.merchant = '7-Eleven'
ORDER BY t.merchant DESC
