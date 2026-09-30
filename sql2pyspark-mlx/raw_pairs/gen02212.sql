SELECT id
FROM transactions
EXCEPT ALL
SELECT id
FROM transactions
WHERE merchant NOT IN ('Shell', 'Grab');
