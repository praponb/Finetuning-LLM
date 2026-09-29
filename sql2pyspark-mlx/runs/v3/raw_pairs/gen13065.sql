SELECT id, is_flagged, account_id
FROM transactions
WHERE is_flagged = FALSE AND (is_flagged OR txn_time < '2025-08-11')
ORDER BY account_id
