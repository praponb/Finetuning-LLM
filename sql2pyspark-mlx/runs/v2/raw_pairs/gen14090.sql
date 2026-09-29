SELECT ss.id, ss.revenue, p.cost, s.region, s.city FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id INNER JOIN stores s ON ss.store_id = s.id ORDER BY city ASC, id DESC;
