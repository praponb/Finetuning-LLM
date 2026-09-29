SELECT AVG(amount) AS avg_amount
FROM transactions
WHERE merchant = 'Shell' AND HOUR(txn_time) BETWEEN 12 AND 19
ORDER BY avg_amount DESC;
