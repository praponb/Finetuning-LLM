SELECT id, merchant, SUBSTRING(txn_type, 1, 4) AS txn_type_prefix
FROM transactions
WHERE amount >= 2800;
