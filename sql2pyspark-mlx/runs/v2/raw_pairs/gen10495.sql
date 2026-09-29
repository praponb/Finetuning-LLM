SELECT id, amount
FROM transactions
WHERE amount = (SELECT MIN(amount) FROM transactions WHERE merchant NOT IN ('7-Eleven', 'Amazon'))
ORDER BY amount DESC;
