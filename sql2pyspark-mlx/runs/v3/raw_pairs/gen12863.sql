SELECT p.order_id, o.channel, s.opened_date, s.city FROM payments p LEFT JOIN orders o ON p.order_id = o.id LEFT JOIN stores s ON s.id = o.store_id;
