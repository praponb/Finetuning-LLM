SELECT * FROM (SELECT txn_type, COUNT(amount) AS amount_count, SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END) AS refund_count FROM transactions GROUP BY txn_type) AS s WHERE refund_count > 43
