SELECT id, is_flagged, txn_time
FROM transactions
ORDER BY txn_time, id
LIMIT 1;
