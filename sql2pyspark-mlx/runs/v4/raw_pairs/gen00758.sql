SELECT s.id, s.cost, o.customer_id, st.city FROM shipments s LEFT JOIN orders o ON s.order_id = o.id LEFT JOIN stores st ON st.id = o.store_id WHERE NOT (s.order_id IS NOT NULL AND s.cost >= 85);
