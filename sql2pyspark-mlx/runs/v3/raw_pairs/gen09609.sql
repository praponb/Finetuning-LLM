SELECT p.id, p.cost, p.supplier_id FROM products p WHERE p.price <= 600 AND EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.revenue IS NOT NULL) ORDER BY cost DESC;
