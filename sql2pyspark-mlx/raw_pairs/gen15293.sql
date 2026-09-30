SELECT o.id, o.order_date, o.store_id, o.shipping_fee, s.region, s.city, s.sqft FROM orders o INNER JOIN stores s ON o.store_id = s.id;
