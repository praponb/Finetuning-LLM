SELECT i.last_restock, i.store_id, s.opened_date, p.category FROM inventory i INNER JOIN stores s ON i.store_id = s.id LEFT JOIN products p ON i.product_id = p.id;
