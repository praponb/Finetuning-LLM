SELECT s.opened_date, ss.product_id, ss.units FROM stores s LEFT JOIN store_sales ss ON ss.store_id = s.id WHERE ss.revenue >= 2100 AND s.opened_date BETWEEN '2024-01-01' AND '2025-05-01'
