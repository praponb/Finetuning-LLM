select *, cast(revenue as int) as revenue_int from store_sales where product_id in (7, 9)
