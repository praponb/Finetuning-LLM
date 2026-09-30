SELECT *, CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE HOUR(txn_time) BETWEEN 10 AND 14;
