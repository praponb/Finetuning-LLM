SELECT ss.id, ss.product_id, ss.units, ss.store_id FROM store_sales ss WHERE ss.store_id IN (SELECT id FROM stores WHERE sqft = 14823);
