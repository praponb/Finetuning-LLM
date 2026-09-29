SELECT i.quantity, i.last_restock, i.product_id, s.opened_date FROM inventory i INNER JOIN stores s ON i.store_id = s.id WHERE i.last_restock > '2023-06-01';
