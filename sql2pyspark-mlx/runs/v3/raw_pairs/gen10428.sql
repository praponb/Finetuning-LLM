SELECT i.id, i.store_id, s.opened_date, s.city FROM inventory i LEFT OUTER JOIN stores s ON i.store_id = s.id ORDER BY id DESC, city ASC;
