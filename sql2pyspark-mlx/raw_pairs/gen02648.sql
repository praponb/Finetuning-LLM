SELECT i.id, i.product_id, i.quantity, p.category, p.price, s.city FROM inventory i INNER JOIN products p ON i.product_id = p.id LEFT JOIN stores s ON i.store_id = s.id WHERE p.cost < 680
