SELECT i.last_restock, i.store_id, p.stock_qty, s.supplier_name, s.rating FROM inventory i FULL OUTER JOIN products p ON i.product_id = p.id FULL OUTER JOIN suppliers s ON p.supplier_id = s.id;
