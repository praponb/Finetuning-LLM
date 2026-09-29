SELECT product_id, sale_date FROM store_sales WHERE store_id IN (2, 6) INTERSECT SELECT product_id, sale_date FROM store_sales WHERE sale_date > '2023-05-13' ORDER BY product_id;
