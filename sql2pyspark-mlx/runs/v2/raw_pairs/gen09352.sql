SELECT s.id, s.sqft, s.region FROM stores s WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.order_date >= '2023-04-01') ORDER BY region, 2
