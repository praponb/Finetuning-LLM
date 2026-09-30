select id, sale_date, store_id, units, round(revenue * 100, 2) revenue_scaled from store_sales where product_id = 1 and product_id in (2, 5, 8, 10)
