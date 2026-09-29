SELECT a.id, a.status, a.shipping_fee, a.discount FROM orders a WHERE a.shipping_fee = (SELECT MIN(b.shipping_fee) FROM orders b WHERE b.status = a.status) ORDER BY a.id
