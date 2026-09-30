SELECT p.method, p.amount, o.channel, s.region, s.sqft FROM payments p INNER JOIN orders o ON p.order_id = o.id INNER JOIN stores s ON s.id = o.store_id;
