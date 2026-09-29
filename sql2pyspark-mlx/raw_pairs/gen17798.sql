SELECT *, CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END AS is_credit, CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
