SELECT
    transactions.id,
    transactions.is_flagged,
    transactions.txn_type,
    accounts.account_type
FROM transactions
INNER JOIN accounts ON transactions.account_id = accounts.id
