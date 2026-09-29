SELECT s.id, s.delivered_date, o.store_id, o.shipping_fee, st.city FROM shipments s LEFT JOIN orders o ON s.order_id = o.id INNER JOIN stores st ON o.store_id = st.id
