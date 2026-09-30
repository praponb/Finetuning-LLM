SELECT id, txn_time, CASE WHEN amount >= 3600 THEN 'High' WHEN amount >= 1000 THEN 'Medium' ELSE 'Low' END AS amount_band FROM transactions
