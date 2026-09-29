SELECT p.id, p.method, o.store_id, o.customer_id, s.sqft FROM payments p LEFT JOIN orders o ON o.id = p.order_id JOIN stores s ON o.store_id = s.id WHERE p.method = 'paypal' ORDER BY id DESC LIMIT 5;
