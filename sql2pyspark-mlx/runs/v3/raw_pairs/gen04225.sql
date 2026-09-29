SELECT QUARTER(txn_time) AS txn_quarter, SUM(amount) AS total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
FROM transactions
GROUP BY QUARTER(txn_time)
