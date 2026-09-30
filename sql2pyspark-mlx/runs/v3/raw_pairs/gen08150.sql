SELECT id, amount, status FROM payments WHERE amount >= (SELECT MAX(amount) FROM payments) ORDER BY id;
