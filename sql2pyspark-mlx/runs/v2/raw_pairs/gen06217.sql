SELECT s.delivered_date, o.discount, o.shipping_fee, st.city, st.opened_date FROM shipments s JOIN orders o ON s.order_id = o.id JOIN stores st ON st.id = o.store_id ORDER BY st.city, delivered_date
