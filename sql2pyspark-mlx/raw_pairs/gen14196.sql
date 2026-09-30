SELECT status, balance, LENGTH(status) AS status_length
FROM accounts
WHERE balance BETWEEN 21000 AND 30000 AND balance > 34000 AND balance BETWEEN 48000 AND 70000
ORDER BY status_length DESC;
