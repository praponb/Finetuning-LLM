SELECT p.id, p.method, p.order_id, o.channel, s.opened_date FROM payments p INNER JOIN orders o ON o.id = p.order_id RIGHT JOIN stores s ON o.store_id = s.id;
