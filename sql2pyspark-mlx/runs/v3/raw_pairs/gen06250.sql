SELECT
    accounts.account_type,
    accounts.status,
    transactions.txn_type,
    transactions.merchant,
    transactions.is_flagged
FROM accounts
LEFT JOIN transactions ON transactions.account_id = accounts.id
WHERE accounts.opened_date >= '2024-09-01'
