SELECT txn_type
FROM transactions
WHERE (is_flagged = TRUE AND txn_type IN ('CREDIT', 'REFUND')) OR is_flagged
