SELECT s.sqft, s.region, s.store_name, o.shipping_fee, o.amount FROM stores s LEFT JOIN orders o ON s.id = o.store_id ORDER BY s.sqft
