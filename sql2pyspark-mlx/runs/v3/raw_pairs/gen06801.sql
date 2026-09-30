SELECT orders.id, orders.order_date FROM orders WHERE orders.store_id IN (SELECT id FROM stores WHERE city NOT IN ('Singapore', 'Bangkok'));
