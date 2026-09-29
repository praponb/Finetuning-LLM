SELECT id, balance, LENGTH(status) AS status_length
FROM accounts
WHERE status IN ('FROZEN', 'OPEN', 'CLOSED') AND account_type IN ('SAVINGS', 'LOAN')
