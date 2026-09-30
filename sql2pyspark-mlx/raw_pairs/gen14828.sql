SELECT txn_type, COUNT(*) AS cnt FROM transactions WHERE is_flagged = TRUE GROUP BY txn_type HAVING SUM(CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END) > 67 ORDER BY txn_type ASC, 2 DESC
