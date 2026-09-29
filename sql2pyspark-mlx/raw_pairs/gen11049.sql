SELECT p.id, p.method, p.status, o.order_date, s.city, s.opened_date FROM payments p LEFT JOIN orders o ON p.order_id = o.id LEFT JOIN stores s ON o.store_id = s.id WHERE o.customer_id IS NULL
