SELECT id, txn_type, is_flagged
FROM transactions
WHERE is_flagged AND amount >= 4000;
