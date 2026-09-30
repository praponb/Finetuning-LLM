SELECT id, amount, method
FROM payments
WHERE amount < (SELECT MIN(amount) FROM payments WHERE HOUR(paid_at) BETWEEN 7 AND 14)
ORDER BY method DESC;
