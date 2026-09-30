SELECT o.id, o.shipping_fee, o.customer_id, o.order_date, s.city, s.opened_date FROM orders o INNER JOIN stores s ON o.store_id = s.id WHERE o.store_id IS NULL;
