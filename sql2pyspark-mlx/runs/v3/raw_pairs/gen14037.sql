SELECT YEAR(txn_time) AS txn_year, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY YEAR(txn_time)) AS cumulative_amount FROM transactions WHERE is_flagged = TRUE GROUP BY YEAR(txn_time)
