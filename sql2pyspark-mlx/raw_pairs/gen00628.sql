SELECT s.cost, s.status, o.status AS order_status, st.region FROM shipments s JOIN orders o ON s.order_id = o.id JOIN stores st ON o.store_id = st.id ORDER BY 3;
