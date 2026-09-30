SELECT o.customer_id, o.shipping_fee, o.store_id, s.store_name, s.city, s.region FROM orders o JOIN stores s ON o.store_id = s.id WHERE o.status <> 'SHIPPED';
