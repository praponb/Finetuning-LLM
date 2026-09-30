SELECT id, account_type, CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END AS is_closed
FROM accounts
