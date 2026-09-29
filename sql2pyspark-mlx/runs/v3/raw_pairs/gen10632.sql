SELECT p.order_id, o.discount, o.channel, s.opened_date FROM payments p INNER JOIN orders o ON p.order_id = o.id INNER JOIN stores s ON o.store_id = s.id;
