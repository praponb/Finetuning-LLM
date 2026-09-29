SELECT s.status, s.delivered_date, o.store_id, c.country FROM shipments s JOIN orders o ON s.order_id = o.id LEFT JOIN customers c ON o.customer_id = c.id WHERE o.customer_id = 26;
