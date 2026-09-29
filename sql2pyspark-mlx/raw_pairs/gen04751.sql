SELECT ss.id, ss.revenue FROM store_sales ss WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE opened_date < '2024-04-01');
