SELECT s.carrier, o.order_date, st.city, st.store_name FROM shipments s RIGHT JOIN orders o ON s.order_id = o.id INNER JOIN stores st ON st.id = o.store_id ORDER BY order_date, city
