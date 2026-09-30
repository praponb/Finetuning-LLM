SELECT i.id, i.product_id, i.store_id, s.opened_date, s.region FROM inventory i LEFT JOIN stores s ON s.id = i.store_id WHERE LENGTH(s.store_name) >= 13
