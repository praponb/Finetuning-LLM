SELECT a.account_type, t.first_txn_time
FROM accounts a
INNER JOIN (SELECT account_id, MIN(txn_time) AS first_txn_time
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
