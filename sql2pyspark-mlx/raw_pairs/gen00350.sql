SELECT txn_type, CAST(is_flagged AS INT) AS is_flagged_flag, SUBSTRING(txn_type, 1, 2) AS txn_type_prefix
FROM transactions
WHERE merchant = 'Amazon' AND amount > 1700
ORDER BY txn_type_prefix ASC, is_flagged_flag ASC
