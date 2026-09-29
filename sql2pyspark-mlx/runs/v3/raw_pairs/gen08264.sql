SELECT ss.id, ss.product_id, p.supplier_id FROM store_sales ss JOIN products p ON ss.product_id = p.id WHERE YEAR(ss.sale_date) = 2024 ORDER BY id DESC LIMIT 1;
