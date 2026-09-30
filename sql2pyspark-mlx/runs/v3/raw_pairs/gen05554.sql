SELECT i.last_restock, s.store_name, s.opened_date FROM inventory i INNER JOIN stores s ON i.store_id = s.id
