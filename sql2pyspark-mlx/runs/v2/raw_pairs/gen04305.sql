SELECT *
FROM transactions
WHERE HOUR(txn_time) BETWEEN 12 AND 17 AND (is_flagged = TRUE OR amount BETWEEN 1100 AND 2100)
ORDER BY id
