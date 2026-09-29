SELECT txn_type, CASE WHEN amount >= 3500 THEN 'high' WHEN amount >= 1500 THEN 'mid' ELSE 'low' END AS amount_bucket, COUNT(CASE WHEN merchant = 'Grab' THEN 1 END) AS grab_count
FROM transactions
WHERE NOT (HOUR(txn_time) BETWEEN 10 AND 16 AND account_id IN (1, 7, 19))
GROUP BY txn_type, CASE WHEN amount >= 3500 THEN 'high' WHEN amount >= 1500 THEN 'mid' ELSE 'low' END
ORDER BY amount_bucket;
