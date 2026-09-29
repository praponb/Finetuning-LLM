SELECT i.store_id, i.product_id, i.quantity, s.opened_date FROM inventory i INNER JOIN stores s ON s.id = i.store_id WHERE i.store_id IN (1, 5) AND i.product_id IN (4, 5) ORDER BY i.store_id ASC;
