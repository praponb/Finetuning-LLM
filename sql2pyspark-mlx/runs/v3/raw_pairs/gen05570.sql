SELECT i.id, i.product_id, i.store_id, i.quantity, p.supplier_id, p.stock_qty, p.launch_date FROM inventory i LEFT JOIN products p ON p.id = i.product_id WHERE i.last_restock >= '2024-08-01';
