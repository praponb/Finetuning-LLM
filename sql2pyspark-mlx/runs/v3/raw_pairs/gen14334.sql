SELECT account_id, CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
ORDER BY is_flagged_flag DESC;
