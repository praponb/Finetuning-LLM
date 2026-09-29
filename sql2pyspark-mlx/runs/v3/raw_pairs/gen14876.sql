SELECT a.id, a.status, COUNT(t.id) AS transaction_count
FROM accounts a
JOIN transactions t ON a.id = t.account_id
WHERE t.txn_time < '2024-03-15'
GROUP BY a.id, a.status;
