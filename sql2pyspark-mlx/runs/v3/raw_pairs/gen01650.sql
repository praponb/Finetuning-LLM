SELECT DISTINCT merchant, txn_type FROM transactions WHERE is_flagged ORDER BY txn_type, merchant LIMIT 10;
