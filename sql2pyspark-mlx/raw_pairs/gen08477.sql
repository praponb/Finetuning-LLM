SELECT amount, method
FROM payments
WHERE amount >= 1340
ORDER BY method DESC
