SELECT i.quantity, s.city, p.cost, p.supplier_id FROM inventory i JOIN stores s ON i.store_id = s.id LEFT JOIN products p ON i.product_id = p.id WHERE p.price < 1030 ORDER BY city
