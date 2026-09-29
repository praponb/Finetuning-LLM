SELECT i.id, i.store_id, i.last_restock, p.cost, p.brand, s.country FROM inventory i JOIN products p ON i.product_id = p.id JOIN suppliers s ON p.supplier_id = s.id WHERE i.quantity = 131;
