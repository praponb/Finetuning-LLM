SELECT a.customer_id, t.amount_count, t.last_txn_time
FROM accounts a
JOIN (SELECT account_id, COUNT(amount) AS amount_count, MAX(txn_time) AS last_txn_time
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.amount_count > 5
ORDER BY t.amount_count DESC;
