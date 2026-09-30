SELECT id, sale_date, product_id FROM store_sales WHERE product_id IN (2, 3) UNION ALL SELECT id, sale_date, product_id FROM store_sales WHERE sale_date BETWEEN '2025-04-01' AND '2025-06-29';
