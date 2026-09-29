SELECT o.amount, o.customer_id, o.store_id, p.status FROM orders o INNER JOIN payments p ON o.id = p.order_id WHERE p.order_id IN (13, 18, 33) AND o.store_id IS NULL;
