SELECT accounts.account_type, MAX(transactions.txn_time) AS latest_txn_time, ROUND(STDDEV(transactions.amount), 2) AS stddev_amount
FROM transactions
INNER JOIN accounts ON transactions.account_id = accounts.id
GROUP BY accounts.account_type
HAVING ROUND(STDDEV(transactions.amount), 2) > 58
ORDER BY stddev_amount DESC
