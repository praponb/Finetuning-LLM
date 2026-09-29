SELECT i.id, i.quantity, s.opened_date, s.region, s.city FROM inventory i INNER JOIN stores s ON i.store_id = s.id WHERE i.product_id IN (4, 6, 7, 9) ORDER BY id DESC LIMIT 5;
