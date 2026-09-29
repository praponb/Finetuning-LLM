SELECT ss.id, ss.revenue, p.brand, s.store_name FROM store_sales ss FULL OUTER JOIN products p ON ss.product_id = p.id LEFT JOIN stores s ON ss.store_id = s.id;
