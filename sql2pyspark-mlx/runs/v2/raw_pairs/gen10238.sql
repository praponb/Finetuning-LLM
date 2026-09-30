SELECT a.id, a.status, a.amount FROM orders a WHERE a.amount = (SELECT MIN(b.amount) FROM orders b WHERE b.status = a.status);
