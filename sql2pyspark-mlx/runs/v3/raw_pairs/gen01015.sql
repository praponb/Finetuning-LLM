SELECT s.id, s.city, s.sqft
FROM stores s
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.shipping_fee <= 30);
