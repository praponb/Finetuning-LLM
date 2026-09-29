SELECT status
FROM accounts
UNION
SELECT status
FROM payments
WHERE method IS NULL;
