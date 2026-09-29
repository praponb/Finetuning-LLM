SELECT txn_type, SUM(amount) total_amount, COUNT(*) cnt, ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total FROM transactions WHERE is_flagged = TRUE GROUP BY txn_type
