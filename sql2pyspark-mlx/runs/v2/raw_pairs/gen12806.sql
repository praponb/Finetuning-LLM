SELECT txn_type, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY txn_type) AS cumulative_amount
FROM transactions
GROUP BY txn_type
ORDER BY total_amount DESC
