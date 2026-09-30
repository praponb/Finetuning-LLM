SELECT ss.id, ss.revenue, ss.sale_date, ss.units FROM store_sales ss WHERE ss.store_id IN (SELECT id FROM stores WHERE city IN ('Bangkok', 'New York')) ORDER BY ss.units, id LIMIT 1
