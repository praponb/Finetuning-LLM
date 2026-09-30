SELECT id, account_id, CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE account_id = 1 AND (amount BETWEEN 2800 AND 3500 OR account_id IN (8, 19))
