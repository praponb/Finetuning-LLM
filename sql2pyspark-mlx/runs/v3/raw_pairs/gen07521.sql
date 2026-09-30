SELECT i.store_id, i.quantity, s.sqft, p.stock_qty, p.cost FROM inventory i JOIN stores s ON i.store_id = s.id JOIN products p ON i.product_id = p.id WHERE s.store_name LIKE '%Riversi%';
