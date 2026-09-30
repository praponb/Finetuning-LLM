SELECT ss.id, ss.sale_date, ss.revenue FROM store_sales ss WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE region IS NULL) ORDER BY revenue DESC
