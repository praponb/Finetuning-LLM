SELECT i.id, i.store_id, p.price, s.store_name FROM inventory i JOIN products p ON i.product_id = p.id RIGHT JOIN stores s ON i.store_id = s.id;
