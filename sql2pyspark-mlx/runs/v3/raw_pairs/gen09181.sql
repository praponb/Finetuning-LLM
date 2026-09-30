SELECT s.id, s.city, s.store_name, s.opened_date FROM stores s WHERE s.region != 'West' AND s.id IN (SELECT store_id FROM orders WHERE order_date > '2023-08-01');
