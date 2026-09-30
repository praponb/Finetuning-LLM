SELECT ss.id, ss.sale_date, ss.revenue, s.sqft, s.opened_date, p.cost FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id LEFT JOIN products p ON ss.product_id = p.id ORDER BY ss.id DESC
