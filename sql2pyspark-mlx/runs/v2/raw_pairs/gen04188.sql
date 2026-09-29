SELECT s.id, s.sqft FROM stores s WHERE LENGTH(s.store_name) > 13 AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.sale_date BETWEEN '2023-12-01' AND '2025-06-17')
