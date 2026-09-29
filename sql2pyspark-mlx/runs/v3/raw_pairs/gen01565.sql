SELECT ss.id, ss.store_id, p.category, s.supplier_name FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id
