SELECT s.delivered_date, o.status, st.city
FROM shipments s
LEFT JOIN orders o ON o.id = s.order_id
LEFT JOIN stores st ON o.store_id = st.id;
