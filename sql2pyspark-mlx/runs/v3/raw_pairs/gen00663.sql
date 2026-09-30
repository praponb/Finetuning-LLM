SELECT ss.id, ss.revenue, ss.units FROM store_sales ss WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE opened_date >= '2024-02-01') ORDER BY id DESC, revenue;
