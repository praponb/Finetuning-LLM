SELECT s.cost, o.amount, st.region FROM shipments s JOIN orders o ON o.id = s.order_id RIGHT JOIN stores st ON o.store_id = st.id WHERE o.customer_id IS NULL;
