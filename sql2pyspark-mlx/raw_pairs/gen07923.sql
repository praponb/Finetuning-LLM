SELECT a.id, a.status, a.amount FROM payments a WHERE a.amount = (SELECT MAX(b.amount) FROM payments b WHERE b.status = a.status) ORDER BY a.id
