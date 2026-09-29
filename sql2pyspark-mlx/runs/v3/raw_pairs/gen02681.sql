SELECT a.account_type, t.min_amount, t.latest_txn_time
FROM accounts a
JOIN (SELECT account_id, MIN(amount) AS min_amount, MAX(txn_time) AS latest_txn_time
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.min_amount > 2900
ORDER BY latest_txn_time DESC;
