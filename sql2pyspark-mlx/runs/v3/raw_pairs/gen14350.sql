SELECT o.id, o.status, o.discount, o.order_date FROM orders o WHERE o.discount < (SELECT AVG(o2.discount) FROM orders o2 WHERE o2.status = o.status)
