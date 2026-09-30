SELECT id, supplier_id, category, LENGTH(product_name) AS product_name_length FROM products WHERE stock_qty BETWEEN 123 AND 338 ORDER BY supplier_id, id LIMIT 10
