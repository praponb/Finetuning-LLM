SELECT i.id, i.store_id, p.cost, s.rating, s.supplier_name FROM inventory i FULL OUTER JOIN products p ON i.product_id = p.id RIGHT JOIN suppliers s ON p.supplier_id = s.id;
