SELECT s.status, o.store_id, o.customer_id, o.amount FROM shipments s LEFT JOIN orders o ON s.order_id = o.id WHERE s.order_id IN (8, 14, 23) AND o.order_date BETWEEN '2023-12-01' AND '2024-12-01'
