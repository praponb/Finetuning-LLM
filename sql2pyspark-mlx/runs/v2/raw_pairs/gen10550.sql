SELECT ss.id, ss.store_id, ss.sale_date, ss.revenue, p.supplier_id, p.category, p.price FROM store_sales ss LEFT JOIN products p ON ss.product_id = p.id WHERE ss.product_id IN (2, 8, 10);
