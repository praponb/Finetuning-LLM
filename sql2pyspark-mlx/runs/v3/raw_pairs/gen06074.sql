select product_id from store_sales where product_id in (3, 5) union select product_id from store_sales where sale_date > '2024-05-01';
