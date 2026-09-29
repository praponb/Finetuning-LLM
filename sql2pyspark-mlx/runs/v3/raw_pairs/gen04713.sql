SELECT p.id, p.amount, p.method, o.customer_id, s.city FROM payments p LEFT OUTER JOIN orders o ON o.id = p.order_id LEFT OUTER JOIN stores s ON o.store_id = s.id;
