SELECT a.id, a.status, a.amount FROM payments a WHERE a.amount = (SELECT MIN(b.amount) FROM payments b WHERE b.status = a.status) ORDER BY a.amount DESC;
