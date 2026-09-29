SELECT p.id, p.paid_at, p.method, o.store_id, o.amount, s.sqft FROM payments p JOIN orders o ON p.order_id = o.id LEFT JOIN stores s ON o.store_id = s.id WHERE o.channel = 'partner';
