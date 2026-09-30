SELECT ss.product_id, p.category, s.country, LENGTH(p.product_name) product_name_length FROM store_sales ss JOIN products p ON ss.product_id = p.id LEFT JOIN suppliers s ON p.supplier_id = s.id
