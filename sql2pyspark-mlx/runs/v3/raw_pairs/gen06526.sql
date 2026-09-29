SELECT p.id, p.product_name, p.launch_date, p.category FROM products p WHERE p.stock_qty = 349 AND p.supplier_id IN (SELECT id FROM suppliers WHERE UPPER(TRIM(supplier_name)) = 'SIAM PARTS CO');
