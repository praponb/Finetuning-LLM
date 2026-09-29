SELECT ss.id, ss.sale_date, p.supplier_id
FROM store_sales ss
RIGHT JOIN products p ON ss.product_id = p.id
WHERE ss.store_id IN (1, 2, 3, 4) AND p.supplier_id IN (14, 27, 28);
