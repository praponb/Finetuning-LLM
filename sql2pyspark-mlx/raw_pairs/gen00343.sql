SELECT o.id, o.customer_id, o.discount, o.store_id, s.city FROM orders o JOIN stores s ON o.store_id = s.id;
