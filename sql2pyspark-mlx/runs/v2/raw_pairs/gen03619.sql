SELECT id, balance, status
FROM accounts
WHERE status IN ('CLOSED', 'FROZEN', 'OPEN') AND account_type <> 'SAVINGS'
ORDER BY balance DESC, id ASC;
