SELECT s.id, s.store_name FROM stores s WHERE s.opened_date <= '2024-01-01' AND EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.shipping_fee >= 36)
