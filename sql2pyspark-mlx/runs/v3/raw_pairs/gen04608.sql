SELECT i.last_restock, i.product_id, p.brand, s.country FROM inventory i INNER JOIN products p ON p.id = i.product_id RIGHT OUTER JOIN suppliers s ON p.supplier_id = s.id ORDER BY last_restock DESC;
