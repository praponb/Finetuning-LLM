SELECT id, amount, status, method FROM payments WHERE amount > (SELECT AVG(amount) FROM payments) ORDER BY status DESC
