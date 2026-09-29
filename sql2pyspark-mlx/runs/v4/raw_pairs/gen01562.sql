SELECT ss.id, ss.units, ss.store_id, ss.revenue FROM store_sales ss WHERE ss.store_id IN (SELECT id FROM stores WHERE region NOT IN ('West', 'South', 'North'));
