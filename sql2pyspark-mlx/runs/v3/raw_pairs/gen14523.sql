SELECT i.id, i.product_id, i.store_id, p.cost, p.price, s.city FROM inventory i INNER JOIN products p ON i.product_id = p.id LEFT JOIN stores s ON i.store_id = s.id ORDER BY p.cost
