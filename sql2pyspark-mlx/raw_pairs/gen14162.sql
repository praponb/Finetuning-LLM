SELECT p.supplier_id, p.product_name, ss.sale_date FROM products p LEFT OUTER JOIN store_sales ss ON p.id = ss.product_id;
