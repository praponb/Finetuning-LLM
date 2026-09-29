SELECT a.id, a.status, a.amount, a.shipping_fee FROM orders a WHERE a.amount > (SELECT AVG(b.amount) FROM orders b WHERE b.status = a.status)
