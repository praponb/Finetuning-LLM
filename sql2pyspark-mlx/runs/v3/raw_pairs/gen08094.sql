SELECT i.id, i.store_id, i.last_restock, p.category, s.country FROM inventory i JOIN products p ON i.product_id = p.id RIGHT OUTER JOIN suppliers s ON s.id = p.supplier_id;
