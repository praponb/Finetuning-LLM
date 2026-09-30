SELECT s.id, s.opened_date FROM stores s WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.order_date <= '2024-12-30')
