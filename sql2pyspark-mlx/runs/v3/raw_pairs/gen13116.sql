SELECT p.id, p.supplier_id, p.cost FROM products p WHERE p.id IN (SELECT product_id FROM store_sales WHERE MONTH(sale_date) IN (6, 7, 11))
