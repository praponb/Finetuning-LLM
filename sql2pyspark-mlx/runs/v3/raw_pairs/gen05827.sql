select sale_date, store_id from store_sales where revenue between 2900 and 3000 union all select sale_date, store_id from store_sales where product_id in (3, 5, 8)
