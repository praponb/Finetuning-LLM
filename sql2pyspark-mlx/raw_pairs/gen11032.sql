SELECT txn_type, SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count FROM transactions GROUP BY txn_type HAVING credit_count > 31 ORDER BY txn_type;
