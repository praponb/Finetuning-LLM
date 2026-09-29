SELECT txn_type, MONTH(txn_time) AS txn_month, SUM(CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END) AS grab_count FROM transactions GROUP BY txn_type, txn_month
