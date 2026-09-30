SELECT id, amount FROM transactions WHERE amount >= (SELECT AVG(amount) FROM transactions) ORDER BY amount DESC, id LIMIT 10;
