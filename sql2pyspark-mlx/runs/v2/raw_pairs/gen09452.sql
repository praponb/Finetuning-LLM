SELECT s.delivered_date, o.shipping_fee, o.order_date, st.city, st.region FROM shipments s JOIN orders o ON s.order_id = o.id JOIN stores st ON st.id = o.store_id ORDER BY region DESC, city DESC
