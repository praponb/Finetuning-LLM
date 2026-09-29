SELECT ss.id, ss.revenue FROM store_sales ss WHERE ss.store_id IN (SELECT id FROM stores WHERE region IN ('Central', 'East'));
