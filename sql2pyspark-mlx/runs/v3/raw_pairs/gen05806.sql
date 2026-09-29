SELECT p.id, p.supplier_id, p.brand, p.category FROM products p WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.revenue <= 4000);
