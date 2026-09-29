SELECT
    transactions.id,
    transactions.is_flagged,
    transactions.txn_time,
    transactions.account_id,
    accounts.balance,
    accounts.opened_date,
    accounts.status
FROM transactions
RIGHT OUTER JOIN accounts ON transactions.account_id = accounts.id
WHERE transactions.txn_type = 'REFUND'
ORDER BY id ASC
