SELECT s.id, s.sqft
FROM stores s
WHERE s.region = 'East' AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.shipping_fee < 36)
ORDER BY sqft DESC
