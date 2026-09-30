SELECT p.id, p.cost, p.brand, p.supplier_id FROM products p WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.revenue <= 1700) ORDER BY id DESC;
