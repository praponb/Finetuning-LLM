SELECT i.id, i.store_id, p.launch_date FROM inventory i JOIN products p ON i.product_id = p.id WHERE p.supplier_id IS NULL AND i.product_id IN (1, 4, 6, 10) ORDER BY launch_date;
