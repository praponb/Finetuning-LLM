SELECT i.id, i.product_id, i.store_id, p.price, p.supplier_id FROM inventory i JOIN products p ON p.id = i.product_id
