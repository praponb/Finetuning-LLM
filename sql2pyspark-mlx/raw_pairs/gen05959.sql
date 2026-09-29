SELECT id, amount, method FROM payments WHERE amount > (SELECT MAX(amount) FROM payments) ORDER BY amount DESC;
