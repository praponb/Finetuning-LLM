SELECT p.order_id, p.method, o.status, o.customer_id, s.sqft FROM payments p FULL OUTER JOIN orders o ON p.order_id = o.id INNER JOIN stores s ON o.store_id = s.id
