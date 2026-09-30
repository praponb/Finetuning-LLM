SELECT id, balance
FROM accounts
WHERE status IS NOT NULL
ORDER BY id;
