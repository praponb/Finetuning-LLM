SELECT i.id, i.quantity, p.price, s.city FROM inventory i FULL OUTER JOIN products p ON i.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id WHERE s.rating < 1.9 AND i.store_id = 5;
