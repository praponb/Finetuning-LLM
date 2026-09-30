SELECT p.id, p.status, o.amount, o.store_id, s.sqft FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id LEFT OUTER JOIN stores s ON s.id = o.store_id
