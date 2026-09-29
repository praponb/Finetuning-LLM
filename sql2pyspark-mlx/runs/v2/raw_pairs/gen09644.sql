SELECT ss.id, ss.revenue, p.supplier_id, p.category, p.price FROM store_sales ss JOIN products p ON ss.product_id = p.id WHERE p.category != 'Sports' AND p.supplier_id = 16 ORDER BY supplier_id
