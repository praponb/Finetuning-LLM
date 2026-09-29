SELECT txn_type, account_id, COUNT(CASE WHEN merchant = 'Grab' THEN 1 END) AS grab_count
FROM transactions
WHERE account_id IN (6, 21, 24)
GROUP BY 1, 2
ORDER BY grab_count, txn_type, account_id
LIMIT 5
