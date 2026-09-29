SELECT status
FROM accounts
WHERE balance <= 72000
EXCEPT
SELECT status
FROM shipments
WHERE cost BETWEEN 28 AND 56
