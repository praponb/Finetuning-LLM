SELECT i.quantity, i.last_restock, p.supplier_id, p.product_name, s.country, s.rating FROM inventory i JOIN products p ON i.product_id = p.id JOIN suppliers s ON p.supplier_id = s.id
