SELECT
    transactions.id,
    transactions.txn_time,
    accounts.account_type,
    accounts.status,
    accounts.customer_id,
    CONCAT_WS(',', accounts.status, accounts.account_type) AS status_account_type_key
FROM transactions
INNER JOIN accounts ON accounts.id = transactions.account_id
WHERE accounts.account_type IN ('SAVINGS', 'LOAN')
ORDER BY transactions.id DESC
LIMIT 1
