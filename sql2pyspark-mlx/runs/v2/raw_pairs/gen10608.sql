SELECT id, product_id FROM store_sales WHERE sale_date >= '2023-12-01' UNION ALL SELECT id, product_id FROM store_sales WHERE store_id = 5 ORDER BY product_id;
