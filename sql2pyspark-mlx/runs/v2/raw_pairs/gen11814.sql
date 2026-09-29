SELECT a.id, a.status, a.shipping_fee, a.order_date FROM orders a WHERE a.shipping_fee < (SELECT AVG(b.shipping_fee) FROM orders b WHERE b.status = a.status) ORDER BY a.status;
