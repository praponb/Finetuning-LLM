SELECT o.id, o.order_date, o.amount, o.channel, s.opened_date, s.region FROM orders o INNER JOIN stores s ON o.store_id = s.id WHERE o.store_id = 18 ORDER BY amount, id LIMIT 3;
