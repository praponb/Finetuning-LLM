SELECT i.quantity, p.category, p.supplier_id, s.store_name FROM inventory i JOIN products p ON i.product_id = p.id LEFT OUTER JOIN stores s ON i.store_id = s.id;
