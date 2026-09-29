select id, product_id, sale_date from store_sales where store_id in (1, 3) union all select id, product_id, sale_date from store_sales where revenue between 1100 and 3600;
