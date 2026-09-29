SELECT store_id, product_id FROM store_sales EXCEPT ALL SELECT store_id, product_id FROM store_sales WHERE sale_date > '2025-10-01';
