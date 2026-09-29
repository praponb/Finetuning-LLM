SELECT id, supplier_id, COUNT(*) OVER (PARTITION BY supplier_id) supplier_id_count FROM products WHERE stock_qty <= 343 ORDER BY supplier_id
