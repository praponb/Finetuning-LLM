SELECT method
FROM payments
WHERE amount > 1130
INTERSECT
SELECT method
FROM payments
WHERE amount BETWEEN 870 AND 1460
