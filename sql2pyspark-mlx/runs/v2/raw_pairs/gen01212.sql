SELECT account_id, AVG(amount) AS avg_amount
FROM transactions
WHERE merchant IN ('Amazon', 'Shell') AND HOUR(txn_time) BETWEEN 6 AND 12
GROUP BY account_id
ORDER BY account_id DESC
