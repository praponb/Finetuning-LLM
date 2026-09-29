SELECT CASE WHEN amount >= 3500 THEN 'high' WHEN amount >= 1500 THEN 'mid' ELSE 'low' END amount_bucket, MIN(txn_time) first_txn_time FROM transactions GROUP BY 1 ORDER BY amount_bucket DESC
