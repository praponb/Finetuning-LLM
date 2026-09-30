SELECT p.order_id, o.status, s.city FROM payments p RIGHT JOIN orders o ON p.order_id = o.id JOIN stores s ON o.store_id = s.id WHERE o.shipping_fee < 27 ORDER BY city
