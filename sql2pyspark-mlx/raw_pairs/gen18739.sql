SELECT i.id, i.product_id, i.store_id, s.opened_date, p.cost FROM inventory i FULL OUTER JOIN stores s ON s.id = i.store_id JOIN products p ON i.product_id = p.id
