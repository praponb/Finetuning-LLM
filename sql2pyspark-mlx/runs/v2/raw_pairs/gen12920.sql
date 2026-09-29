SELECT ss.product_id, p.supplier_id FROM store_sales ss LEFT JOIN products p ON ss.product_id = p.id WHERE ss.sale_date < '2025-07-14'
