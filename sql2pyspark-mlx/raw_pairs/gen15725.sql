SELECT p.method, o.amount, o.store_id, o.order_date FROM payments p JOIN orders o ON p.order_id = o.id WHERE o.customer_id IN (7, 26, 36)
