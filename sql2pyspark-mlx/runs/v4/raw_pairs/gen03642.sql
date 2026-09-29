SELECT status
FROM accounts
WHERE balance IS NOT NULL
UNION ALL
SELECT status
FROM payments;
