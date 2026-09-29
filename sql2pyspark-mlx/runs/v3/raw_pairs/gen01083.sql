SELECT ss.product_id, s.store_name, s.region, p.category, p.brand FROM store_sales ss JOIN stores s ON ss.store_id = s.id JOIN products p ON ss.product_id = p.id WHERE s.region IS NULL;
