SELECT id, amount FROM payments WHERE amount >= (SELECT AVG(amount) FROM payments WHERE status = 'SUCCESS') ORDER BY amount DESC, id LIMIT 3;
