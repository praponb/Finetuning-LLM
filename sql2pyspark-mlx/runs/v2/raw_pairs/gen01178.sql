SELECT o.shipping_fee, s.region, s.sqft FROM orders o JOIN stores s ON o.store_id = s.id WHERE s.region = 'Central' AND o.amount IS NULL ORDER BY shipping_fee DESC
