SELECT s.id, s.shipped_date, o.discount, st.city FROM shipments s INNER JOIN orders o ON s.order_id = o.id INNER JOIN stores st ON st.id = o.store_id WHERE o.discount IS NOT NULL;
