SELECT ss.id, ss.sale_date FROM store_sales ss WHERE ss.store_id IN (SELECT id FROM stores WHERE region IN ('East', 'South')) ORDER BY id DESC LIMIT 5;
