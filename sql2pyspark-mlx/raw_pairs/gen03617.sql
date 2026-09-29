SELECT status
FROM payments
INTERSECT
SELECT status
FROM payments
WHERE amount < 1410
