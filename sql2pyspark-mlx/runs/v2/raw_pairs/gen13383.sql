SELECT i.product_id, i.quantity, i.store_id, p.supplier_id, p.brand, p.cost FROM inventory i LEFT OUTER JOIN products p ON i.product_id = p.id ORDER BY i.store_id DESC;
