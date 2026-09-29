SELECT ss.units, ss.store_id, s.region, p.cost FROM store_sales ss INNER JOIN stores s ON ss.store_id = s.id INNER JOIN products p ON ss.product_id = p.id ORDER BY region
