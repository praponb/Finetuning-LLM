SELECT a.account_type, a.opened_date, t.min_txn_time
FROM accounts a
INNER JOIN (SELECT account_id, MIN(txn_time) min_txn_time
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
ORDER BY account_type, opened_date;
