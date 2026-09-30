SELECT ss.id, ss.store_id, ss.revenue FROM store_sales ss WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE opened_date BETWEEN '2024-01-01' AND '2025-02-19')
