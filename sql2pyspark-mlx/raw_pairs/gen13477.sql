SELECT id, amount, paid_at
FROM payments
WHERE amount < (SELECT MIN(amount) FROM payments);
