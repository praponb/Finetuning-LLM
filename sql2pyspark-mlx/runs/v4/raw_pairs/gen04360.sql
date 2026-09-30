SELECT ss.id, ss.store_id, ss.revenue, ss.product_id, s.sqft, ROUND(s.sqft * 0.85, 2) AS adjusted_sqft FROM store_sales ss LEFT JOIN stores s ON ss.store_id = s.id
