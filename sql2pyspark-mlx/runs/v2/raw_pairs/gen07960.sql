SELECT i.id, i.product_id, p.product_name, s.region FROM inventory i JOIN products p ON i.product_id = p.id JOIN stores s ON s.id = i.store_id;
