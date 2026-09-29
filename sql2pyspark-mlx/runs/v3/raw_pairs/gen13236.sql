SELECT method
FROM payments
WHERE method <> 'cash'
UNION
SELECT method
FROM payments
WHERE amount < 710
ORDER BY method;
