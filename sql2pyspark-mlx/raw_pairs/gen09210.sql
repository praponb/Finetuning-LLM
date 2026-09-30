SELECT id, merchant
FROM transactions
WHERE is_flagged = FALSE AND txn_time >= '2025-03-01' AND NOT is_flagged;
