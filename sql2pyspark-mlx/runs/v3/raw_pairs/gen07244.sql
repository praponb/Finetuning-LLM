SELECT s.opened_date, s.sqft, ss.units, ss.revenue, ss.product_id FROM stores s JOIN store_sales ss ON s.id = ss.store_id;
