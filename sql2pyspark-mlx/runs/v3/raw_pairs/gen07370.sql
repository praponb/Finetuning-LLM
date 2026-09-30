SELECT o.customer_id, o.store_id, p.method
FROM orders o
LEFT JOIN payments p ON o.id = p.order_id;
