SELECT a.id, a.status, a.amount, a.method FROM payments a WHERE a.amount > (SELECT AVG(b.amount) FROM payments b WHERE b.status = a.status);
