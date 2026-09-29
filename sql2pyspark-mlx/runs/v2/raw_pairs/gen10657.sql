SELECT a.opened_date, t.latest_txn_time
FROM accounts a
LEFT JOIN (SELECT account_id, MAX(txn_time) AS latest_txn_time
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id;
