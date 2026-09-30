SELECT a.id, a.status, a.amount, a.order_id FROM payments a WHERE a.amount < (SELECT AVG(b.amount) FROM payments b WHERE b.status = a.status) ORDER BY a.id DESC;
