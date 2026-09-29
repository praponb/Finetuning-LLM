SELECT p.amount, o.channel, s.opened_date, s.store_name FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id INNER JOIN stores s ON o.store_id = s.id;
