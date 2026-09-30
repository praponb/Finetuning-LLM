SELECT
    transactions.amount,
    transactions.is_flagged,
    transactions.txn_time,
    accounts.status,
    LENGTH(transactions.merchant) AS merchant_length
FROM transactions
JOIN accounts ON transactions.account_id = accounts.id
WHERE transactions.account_id IN (16, 19, 32, 40) AND accounts.balance > 62000
ORDER BY transactions.amount;
