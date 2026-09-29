SELECT p.paid_at, o.discount, o.channel, s.city, s.store_name FROM payments p INNER JOIN orders o ON p.order_id = o.id INNER JOIN stores s ON o.store_id = s.id
