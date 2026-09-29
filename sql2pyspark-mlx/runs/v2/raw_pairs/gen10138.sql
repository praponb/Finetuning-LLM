SELECT txn_time, CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END AS is_shell
FROM transactions
WHERE amount BETWEEN 1000 AND 1500 AND merchant IS NULL AND merchant IN ('Grab', '7-Eleven')
ORDER BY is_shell DESC;
