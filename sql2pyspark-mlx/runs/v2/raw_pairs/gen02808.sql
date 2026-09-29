SELECT ss.id, ss.sale_date, ss.revenue, ss.product_id, s.opened_date, s.city, s.sqft FROM store_sales ss LEFT JOIN stores s ON s.id = ss.store_id ORDER BY product_id DESC
