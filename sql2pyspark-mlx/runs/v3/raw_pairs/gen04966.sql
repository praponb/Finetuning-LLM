select store_id, sale_date from store_sales where units = 32 except select store_id, sale_date from store_sales where product_id = 8
