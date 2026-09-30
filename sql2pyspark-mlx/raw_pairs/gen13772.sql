SELECT i.product_id, s.opened_date, p.category FROM inventory i INNER JOIN stores s ON s.id = i.store_id LEFT JOIN products p ON i.product_id = p.id WHERE i.product_id = 9
