SELECT ss.id, ss.product_id, ss.sale_date, p.brand, p.product_name, p.supplier_id FROM store_sales ss JOIN products p ON p.id = ss.product_id;
