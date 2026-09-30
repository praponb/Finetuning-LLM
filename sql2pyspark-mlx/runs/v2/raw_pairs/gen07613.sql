SELECT s.id, s.delivered_date, s.status, o.order_date, st.sqft, st.store_name FROM shipments s JOIN orders o ON o.id = s.order_id JOIN stores st ON o.store_id = st.id;
