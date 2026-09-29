SELECT id, txn_type, COUNT(*) OVER (PARTITION BY txn_type) AS txn_type_count
FROM transactions;
