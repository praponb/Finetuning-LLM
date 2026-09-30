SELECT ss.store_id, ss.units, p.cost, p.category, s.region, s.opened_date FROM store_sales ss JOIN products p ON p.id = ss.product_id JOIN stores s ON ss.store_id = s.id ORDER BY ss.units DESC;
