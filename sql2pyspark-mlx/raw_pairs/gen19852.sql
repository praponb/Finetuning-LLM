SELECT o.id, o.amount, o.status, o.channel, s.region FROM orders o INNER JOIN stores s ON o.store_id = s.id ORDER BY region;
