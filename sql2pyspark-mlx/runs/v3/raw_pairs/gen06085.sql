SELECT is_flagged, AVG(amount) AS avg_amount, SUM(CASE WHEN txn_type = 'DEBIT' THEN 1 ELSE 0 END) AS debit_count FROM transactions WHERE txn_type = 'REFUND' GROUP BY is_flagged
