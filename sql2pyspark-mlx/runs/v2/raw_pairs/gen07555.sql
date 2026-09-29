SELECT s.id, s.city FROM stores s WHERE s.region NOT IN ('West', 'South') AND EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.sale_date >= '2024-03-18');
